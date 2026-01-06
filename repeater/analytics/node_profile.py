"""
Node Profile - Comprehensive single-node analytics
===================================================

Aggregates all available analytics data about a single node into a
unified profile. This provides a complete picture of a node's role,
connectivity, activity patterns, and health in the mesh network.

Use Cases
---------
    - Node detail view in the frontend
    - Troubleshooting node connectivity issues
    - Understanding a node's importance to network health
    - Identifying nodes that need attention

Profile Components
------------------
    Identity:
        - Hash, prefix, name (if known from adverts)
        - Is repeater, contact type
        
    Network Role:
        - Classification: edge, relay, hub, or backbone
        - Is articulation point (critical node)
        - Community membership
        
    Connectivity:
        - Degree (number of connections)
        - Direct neighbors list
        - Centrality score
        - Eccentricity
        
    Traffic:
        - Paths through this node
        - Average hop position
        - Traffic volume (packets)
        
    Activity:
        - Sparkline data
        - Last seen timestamp
        - Active hours count
        
    Signal Quality (if available):
        - Average RSSI
        - Average SNR
        - Zero-hop status

API Endpoint
------------
    GET /api/analytics/node_profile?node=0xAB
    GET /api/analytics/node_profile?node=AB  (prefix also works)
    
    Returns comprehensive NodeProfile object.

See Also
--------
    - graph.py: Centrality and connectivity calculations
    - mobile_detection.py: Mobility classification
    - node_activity.py: Sparkline data
    - topology.py: Hub/gateway classification
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any

from .db import AnalyticsDB, ensure_connection, DBConnection
from .graph import build_graph_from_db, MeshGraph, NETWORKX_AVAILABLE
from .node_activity import get_sparkline_data, ActivityBucket
from .utils import normalize_hash, get_prefix as get_hash_prefix

logger = logging.getLogger("Analytics.NodeProfile")

# Role classification thresholds
HUB_CENTRALITY_THRESHOLD = 0.3
RELAY_CENTRALITY_THRESHOLD = 0.1
HIGH_DEGREE_THRESHOLD = 5


@dataclass
class NodeNeighbor:
    """A neighbor of the profiled node."""
    hash: str
    prefix: str
    edge_key: str
    weight: int  # certain_count
    avg_confidence: float
    last_seen: float
    
    def to_dict(self) -> dict:
        return {
            "hash": self.hash,
            "prefix": self.prefix,
            "edgeKey": self.edge_key,
            "weight": self.weight,
            "avgConfidence": round(self.avg_confidence, 3),
            "lastSeen": self.last_seen,
        }


@dataclass
class SignalQuality:
    """Signal quality metrics for a node."""
    avg_rssi: Optional[float] = None
    avg_snr: Optional[float] = None
    min_rssi: Optional[float] = None
    max_rssi: Optional[float] = None
    measurement_count: int = 0
    is_zero_hop: bool = False
    
    def to_dict(self) -> dict:
        return {
            "avgRssi": round(self.avg_rssi, 1) if self.avg_rssi else None,
            "avgSnr": round(self.avg_snr, 1) if self.avg_snr else None,
            "minRssi": round(self.min_rssi, 1) if self.min_rssi else None,
            "maxRssi": round(self.max_rssi, 1) if self.max_rssi else None,
            "measurementCount": self.measurement_count,
            "isZeroHop": self.is_zero_hop,
        }


@dataclass
class NodeProfile:
    """Comprehensive profile of a single node."""
    # Identity
    hash: str
    prefix: str
    name: Optional[str] = None
    is_repeater: bool = False
    contact_type: Optional[str] = None
    
    # Network Role
    role: str = "unknown"  # edge, relay, hub, backbone
    is_hub: bool = False
    is_gateway: bool = False
    is_articulation_point: bool = False
    is_mobile: bool = False
    community_id: Optional[int] = None
    
    # Connectivity
    degree: int = 0
    centrality: float = 0.0
    eccentricity: Optional[int] = None
    neighbors: List[NodeNeighbor] = field(default_factory=list)
    
    # Traffic
    paths_through: int = 0
    avg_hop_position: float = 0.0
    packet_count: int = 0
    
    # Activity
    sparkline: List[dict] = field(default_factory=list)
    last_seen: float = 0.0
    first_seen: float = 0.0
    active_hours: int = 0
    
    # Signal Quality
    signal_quality: Optional[SignalQuality] = None
    
    # Location
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    # Metadata
    computed_at: float = field(default_factory=time.time)
    
    def to_dict(self) -> dict:
        return {
            "hash": self.hash,
            "prefix": self.prefix,
            "name": self.name,
            "isRepeater": self.is_repeater,
            "contactType": self.contact_type,
            "role": self.role,
            "isHub": self.is_hub,
            "isGateway": self.is_gateway,
            "isArticulationPoint": self.is_articulation_point,
            "isMobile": self.is_mobile,
            "communityId": self.community_id,
            "degree": self.degree,
            "centrality": round(self.centrality, 4),
            "eccentricity": self.eccentricity,
            "neighbors": [n.to_dict() for n in self.neighbors],
            "pathsThrough": self.paths_through,
            "avgHopPosition": round(self.avg_hop_position, 2),
            "packetCount": self.packet_count,
            "sparkline": self.sparkline,
            "lastSeen": self.last_seen,
            "firstSeen": self.first_seen,
            "activeHours": self.active_hours,
            "signalQuality": self.signal_quality.to_dict() if self.signal_quality else None,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "computedAt": self.computed_at,
        }



def _get_node_from_adverts(conn, node_hash: str, prefix: str) -> dict:
    """Get node info from adverts table."""
    cursor = conn.cursor()
    
    # Try exact pubkey match first
    cursor.execute("""
        SELECT pubkey, node_name, is_repeater, contact_type, latitude, longitude,
               first_seen, last_seen, rssi, snr, zero_hop
        FROM adverts
        WHERE pubkey = ? OR pubkey LIKE ?
        ORDER BY last_seen DESC
        LIMIT 1
    """, (node_hash, f"{prefix}%"))
    
    row = cursor.fetchone()
    if row:
        return {
            "pubkey": row[0],
            "name": row[1],
            "is_repeater": bool(row[2]),
            "contact_type": row[3],
            "latitude": row[4],
            "longitude": row[5],
            "first_seen": row[6],
            "last_seen": row[7],
            "rssi": row[8],
            "snr": row[9],
            "zero_hop": bool(row[10]) if row[10] is not None else False,
        }
    return {}


def _get_signal_quality(conn, node_hash: str, prefix: str, hours: int = 168) -> SignalQuality:
    """Get signal quality metrics for a node."""
    cutoff = time.time() - (hours * 3600)
    cursor = conn.cursor()
    
    # Get signal stats from packets where this node was the source
    cursor.execute("""
        SELECT 
            AVG(rssi) as avg_rssi,
            AVG(snr) as avg_snr,
            MIN(rssi) as min_rssi,
            MAX(rssi) as max_rssi,
            COUNT(*) as count
        FROM packets
        WHERE timestamp > ?
          AND src_hash LIKE ?
          AND rssi IS NOT NULL
    """, (cutoff, f"%{prefix}%"))
    
    row = cursor.fetchone()
    
    # Also check adverts for zero-hop info
    cursor.execute("""
        SELECT rssi, snr, zero_hop
        FROM adverts
        WHERE pubkey LIKE ?
        ORDER BY last_seen DESC
        LIMIT 1
    """, (f"%{prefix}%",))
    
    advert_row = cursor.fetchone()
    
    sq = SignalQuality()
    
    if row and row[4] > 0:
        sq.avg_rssi = row[0]
        sq.avg_snr = row[1]
        sq.min_rssi = row[2]
        sq.max_rssi = row[3]
        sq.measurement_count = row[4]
    
    if advert_row:
        # Prefer advert signal data if available (more reliable for zero-hop)
        if advert_row[2]:  # is zero_hop
            sq.is_zero_hop = True
            if advert_row[0] is not None:
                sq.avg_rssi = advert_row[0]
            if advert_row[1] is not None:
                sq.avg_snr = advert_row[1]
    
    return sq


def _get_paths_through_node(conn, prefix: str, hours: int = 168) -> Tuple[int, float]:
    """Count paths that pass through this node and calculate avg position."""
    import json
    
    cutoff = time.time() - (hours * 3600)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT original_path, forwarded_path
        FROM packets
        WHERE timestamp > ?
          AND (original_path IS NOT NULL OR forwarded_path IS NOT NULL)
    """, (cutoff,))
    
    path_count = 0
    position_sum = 0.0
    position_count = 0
    
    for row in cursor.fetchall():
        raw_path = row[1] or row[0]
        if not raw_path:
            continue
        
        try:
            if isinstance(raw_path, str):
                path = json.loads(raw_path)
            else:
                path = raw_path
            
            # Check if this node is in the path
            for i, hop in enumerate(path):
                hop_str = str(hop).upper()
                if hop_str == prefix or hop_str == prefix.lstrip("0X"):
                    path_count += 1
                    # Position from end (0 = last hop)
                    position = len(path) - 1 - i
                    position_sum += position
                    position_count += 1
                    break
        except (json.JSONDecodeError, TypeError):
            continue
    
    avg_position = position_sum / position_count if position_count > 0 else 0.0
    return path_count, avg_position


def _get_packet_count(conn, prefix: str, hours: int = 168) -> int:
    """Count packets involving this node."""
    cutoff = time.time() - (hours * 3600)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT COUNT(*)
        FROM packets
        WHERE timestamp > ?
          AND (src_hash LIKE ? OR original_path LIKE ? OR forwarded_path LIKE ?)
    """, (cutoff, f"%{prefix}%", f"%{prefix}%", f"%{prefix}%"))
    
    return cursor.fetchone()[0]


def _classify_role(
    centrality: float,
    degree: int,
    is_articulation_point: bool,
    is_hub: bool,
    is_gateway: bool,
) -> str:
    """Classify node's network role."""
    if is_hub or centrality >= HUB_CENTRALITY_THRESHOLD:
        if is_articulation_point:
            return "backbone"
        return "hub"
    elif is_gateway or centrality >= RELAY_CENTRALITY_THRESHOLD:
        return "relay"
    elif degree >= HIGH_DEGREE_THRESHOLD:
        return "relay"
    else:
        return "edge"


def get_node_profile(
    conn_or_db: DBConnection,
    node: str,
    neighbors: Optional[Dict[str, dict]] = None,
    hub_nodes: Optional[List[str]] = None,
    gateway_nodes: Optional[List[str]] = None,
    local_hash: Optional[str] = None,
    hours: int = 168,
    min_certainty: int = 5,
) -> Optional[NodeProfile]:
    """
    Build comprehensive profile for a single node.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        node: Node hash or prefix (e.g., "0xAB123456" or "AB")
        neighbors: Known neighbors dict (optional, for name lookup)
        hub_nodes: List of hub node hashes (optional)
        gateway_nodes: List of gateway node hashes (optional)
        local_hash: Local node's hash (optional)
        hours: Time range for activity analysis
        min_certainty: Minimum certainty for graph edges
        
    Returns:
        NodeProfile or None if node not found
    """
    if not NETWORKX_AVAILABLE:
        logger.warning("NetworkX not available, limited profile data")
    
    # Normalize input using centralized utilities
    node_hash = normalize_hash(node)
    prefix = get_hash_prefix(node_hash) or node.upper()[:2]
    
    with ensure_connection(conn_or_db) as conn:
        profile = NodeProfile(
            hash=node_hash,
            prefix=prefix,
        )
        
        # Get advert info
        advert_info = _get_node_from_adverts(conn, node_hash, prefix)
        if advert_info:
            profile.name = advert_info.get("name")
            profile.is_repeater = advert_info.get("is_repeater", False)
            profile.contact_type = advert_info.get("contact_type")
            profile.latitude = advert_info.get("latitude")
            profile.longitude = advert_info.get("longitude")
            profile.first_seen = advert_info.get("first_seen", 0)
            profile.last_seen = advert_info.get("last_seen", 0)
        
        # Check neighbors dict for more info
        if neighbors:
            for nhash, ninfo in neighbors.items():
                if get_hash_prefix(nhash) == prefix:
                    profile.hash = nhash  # Use full hash if available
                    if not profile.name:
                        profile.name = ninfo.get("node_name")
                    if not profile.is_repeater:
                        profile.is_repeater = ninfo.get("is_repeater", False)
                    break
        
        # Build graph for connectivity analysis
        if NETWORKX_AVAILABLE:
            try:
                graph = build_graph_from_db(conn, min_certainty, local_hash)
                
                # Find this node in graph (may be stored with different format)
                graph_node = None
                for gn in graph.graph.nodes():
                    if get_hash_prefix(gn) == prefix:
                        graph_node = gn
                        break
                
                if graph_node:
                    # Connectivity metrics
                    profile.degree = graph.degree(graph_node)
                    profile.eccentricity = graph.eccentricity(graph_node)
                    
                    # Centrality
                    centrality_result = graph.betweenness_centrality()
                    profile.centrality = centrality_result.scores.get(graph_node, 0)
                    
                    # Articulation point check
                    articulation_pts = graph.find_articulation_points()
                    profile.is_articulation_point = graph_node in articulation_pts
                    
                    # Community detection
                    communities = graph.detect_communities()
                    for i, community in enumerate(communities):
                        if graph_node in community:
                            profile.community_id = i
                            break
                    
                    # Get neighbors from graph
                    graph_neighbors = graph.neighbors(graph_node)
                    for neighbor in graph_neighbors:
                        edge_data = graph.graph.get_edge_data(graph_node, neighbor, {})
                        profile.neighbors.append(NodeNeighbor(
                            hash=neighbor,
                            prefix=get_hash_prefix(neighbor),
                            edge_key=graph.get_edge_key(graph_node, neighbor) or "",
                            weight=edge_data.get("certain_count", 0),
                            avg_confidence=edge_data.get("avg_confidence", 0),
                            last_seen=edge_data.get("last_seen", 0),
                        ))
            except Exception as e:
                logger.warning(f"Graph analysis failed for {node}: {e}")
        
        # Hub/gateway classification
        hub_nodes = hub_nodes or []
        gateway_nodes = gateway_nodes or []
        
        profile.is_hub = any(get_hash_prefix(h) == prefix for h in hub_nodes)
        profile.is_gateway = any(get_hash_prefix(h) == prefix for h in gateway_nodes)
        
        # Role classification
        profile.role = _classify_role(
            profile.centrality,
            profile.degree,
            profile.is_articulation_point,
            profile.is_hub,
            profile.is_gateway,
        )
        
        # Signal quality
        profile.signal_quality = _get_signal_quality(conn, node_hash, prefix, hours)
        
        # Path and traffic analysis
        profile.paths_through, profile.avg_hop_position = _get_paths_through_node(conn, prefix, hours)
        profile.packet_count = _get_packet_count(conn, prefix, hours)
        
        # Activity sparkline
        try:
            sparkline_data = get_sparkline_data(conn, [node_hash], hours)
            if node_hash in sparkline_data:
                profile.sparkline = [b.to_dict() for b in sparkline_data[node_hash]]
                profile.active_hours = sum(1 for b in sparkline_data[node_hash] if b.packet_count > 0)
        except Exception as e:
            logger.warning(f"Failed to get sparkline for {node}: {e}")
    
    return profile


def get_node_profiles_batch(
    conn_or_db: DBConnection,
    nodes: List[str],
    **kwargs,
) -> List[NodeProfile]:
    """
    Get profiles for multiple nodes efficiently.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        nodes: List of node hashes or prefixes
        **kwargs: Additional arguments passed to get_node_profile
        
    Returns:
        List of NodeProfile objects (may be shorter than input if some not found)
    """
    profiles = []
    
    for node in nodes:
        profile = get_node_profile(conn_or_db, node, **kwargs)
        if profile:
            profiles.append(profile)
    
    return profiles
