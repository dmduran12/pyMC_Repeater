"""
Route Simulation - Predict and analyze mesh routing behavior
============================================================

Simulates and predicts routing behavior between nodes in the mesh network.
Useful for understanding how packets will be routed and identifying
potential bottlenecks before they become problems.

Use Cases
---------
    - Predict path between two nodes before sending a message
    - Understand why packets take certain routes
    - Identify bottlenecks and weak links on a path
    - Compare alternative routes for reliability
    - Estimate message delivery latency

Simulation Components
---------------------
    Path Prediction:
        Uses historical path data and graph analysis to predict
        the most likely route a packet will take.
        
    Latency Estimation:
        Estimates end-to-end latency based on hop count and
        historical timing data.
        
    Success Probability:
        Calculates probability of successful delivery based on
        path health, link confidence, and historical success rates.
        
    Bottleneck Detection:
        Identifies nodes and links on the path that are likely
        to cause delays or failures.

Routing Model
-------------
MeshCore uses a combination of flood and direct routing:

    Flood Routing:
        - Packet broadcasted to all neighbors
        - Each hop rebroadcasts (with TTL limit)
        - Multiple paths possible
        
    Direct Routing:
        - Packet follows learned path to destination
        - More efficient but requires path knowledge
        - Falls back to flood if path unknown

API Endpoint
------------
    GET /api/analytics/simulate_route?from=0xAB&to=0xCD
    GET /api/analytics/simulate_route?from=0xAB&to=0xCD&via=0xEF
    
    Returns RouteSimulation with predicted path, alternatives,
    estimated latency, and bottleneck analysis.

See Also
--------
    - graph.py: Path finding algorithms
    - path_registry.py: Historical path data
    - network_health.py: Link health assessment
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set

from .db import AnalyticsDB, ensure_connection, DBConnection
from .graph import build_graph_from_db, MeshGraph, NETWORKX_AVAILABLE, PathResult
from .path_registry import get_canonical_paths, ObservedPath
from .utils import normalize_hash, get_prefix as get_hash_prefix, make_edge_key

logger = logging.getLogger("Analytics.RouteSimulation")

# Latency estimation constants (milliseconds)
BASE_LATENCY_PER_HOP_MS = 250  # Base latency per hop
PROCESSING_DELAY_MS = 50       # Per-node processing delay
TX_DELAY_AVG_MS = 100          # Average transmission delay
FLOOD_OVERHEAD_FACTOR = 1.5    # Flood routing adds overhead


@dataclass
class Bottleneck:
    """A potential bottleneck on a route."""
    node: Optional[str] = None
    edge: Optional[str] = None
    reason: str = ""
    severity: str = "low"  # low, medium, high
    impact_factor: float = 1.0  # Multiplier for latency/failure probability
    
    def to_dict(self) -> dict:
        return {
            "node": self.node,
            "edge": self.edge,
            "reason": self.reason,
            "severity": self.severity,
            "impactFactor": round(self.impact_factor, 2),
        }


@dataclass
class AlternativePath:
    """An alternative route to the destination."""
    path: List[str]
    hop_count: int
    estimated_latency_ms: float
    success_probability: float
    is_historical: bool = False  # True if this path was actually observed
    observation_count: int = 0
    
    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "hopCount": self.hop_count,
            "estimatedLatencyMs": round(self.estimated_latency_ms, 0),
            "successProbability": round(self.success_probability, 3),
            "isHistorical": self.is_historical,
            "observationCount": self.observation_count,
        }


@dataclass
class RouteSimulation:
    """Complete route simulation result."""
    source: str
    target: str
    via: Optional[str] = None  # Optional waypoint
    
    # Predicted primary path
    predicted_path: List[str] = field(default_factory=list)
    hop_count: int = 0
    
    # Timing estimates
    estimated_latency_ms: float = 0.0
    estimated_latency_range: Tuple[float, float] = (0.0, 0.0)
    
    # Success metrics
    success_probability: float = 0.0
    confidence: float = 0.0  # Confidence in the prediction
    
    # Alternative paths
    alternative_paths: List[AlternativePath] = field(default_factory=list)
    
    # Bottleneck analysis
    bottlenecks: List[Bottleneck] = field(default_factory=list)
    
    # Route type prediction
    predicted_route_type: str = "flood"  # flood, direct, or unknown
    
    # Metadata
    path_found: bool = False
    error: Optional[str] = None
    computed_at: float = field(default_factory=time.time)
    
    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "target": self.target,
            "via": self.via,
            "predictedPath": self.predicted_path,
            "hopCount": self.hop_count,
            "estimatedLatencyMs": round(self.estimated_latency_ms, 0),
            "estimatedLatencyRange": [round(x, 0) for x in self.estimated_latency_range],
            "successProbability": round(self.success_probability, 3),
            "confidence": round(self.confidence, 3),
            "alternativePaths": [p.to_dict() for p in self.alternative_paths],
            "bottlenecks": [b.to_dict() for b in self.bottlenecks],
            "predictedRouteType": self.predicted_route_type,
            "pathFound": self.path_found,
            "error": self.error,
            "computedAt": self.computed_at,
        }


# Use centralized normalize_hash from utils
_normalize_hash = normalize_hash


def _estimate_latency(
    hop_count: int,
    is_flood: bool = True,
    bottleneck_factor: float = 1.0,
) -> Tuple[float, Tuple[float, float]]:
    """
    Estimate latency for a path.
    
    Returns:
        Tuple of (estimated_ms, (min_ms, max_ms))
    """
    base_latency = hop_count * BASE_LATENCY_PER_HOP_MS
    processing = hop_count * PROCESSING_DELAY_MS
    tx_delay = hop_count * TX_DELAY_AVG_MS
    
    total = base_latency + processing + tx_delay
    
    if is_flood:
        total *= FLOOD_OVERHEAD_FACTOR
    
    total *= bottleneck_factor
    
    # Range is +/- 30%
    min_latency = total * 0.7
    max_latency = total * 1.3
    
    return total, (min_latency, max_latency)


def _calculate_success_probability(
    graph: MeshGraph,
    path: List[str],
    edge_confidence: Dict[str, float],
) -> float:
    """
    Calculate probability of successful delivery along a path.
    
    Based on edge confidence scores - chain rule of probability.
    """
    if len(path) < 2:
        return 1.0 if len(path) == 1 else 0.0
    
    probability = 1.0
    
    for i in range(len(path) - 1):
        from_h = path[i]
        to_h = path[i + 1]
        edge_key = make_edge_key(from_h, to_h)
        
        # Get edge confidence, default to 0.5 if unknown
        confidence = edge_confidence.get(edge_key, 0.5)
        probability *= confidence
    
    return probability


def _detect_bottlenecks(
    graph: MeshGraph,
    path: List[str],
    edge_confidence: Dict[str, float],
    articulation_points: Set[str],
    bridge_edges: Set[Tuple[str, str]],
) -> List[Bottleneck]:
    """Detect bottlenecks along a path."""
    bottlenecks = []
    
    if len(path) < 2:
        return bottlenecks
    
    for i, node in enumerate(path):
        # Check if node is an articulation point
        if node in articulation_points and i > 0 and i < len(path) - 1:
            bottlenecks.append(Bottleneck(
                node=node,
                reason=f"Critical node - network depends on {get_hash_prefix(node)}",
                severity="high",
                impact_factor=1.3,
            ))
    
    for i in range(len(path) - 1):
        from_h = path[i]
        to_h = path[i + 1]
        edge_key = make_edge_key(from_h, to_h)
        
        # Check if edge is a bridge
        if (from_h, to_h) in bridge_edges or (to_h, from_h) in bridge_edges:
            bottlenecks.append(Bottleneck(
                edge=edge_key,
                reason=f"Critical link - no alternate path around {get_hash_prefix(from_h)}-{get_hash_prefix(to_h)}",
                severity="high",
                impact_factor=1.2,
            ))
        
        # Check edge confidence
        confidence = edge_confidence.get(edge_key, 0.5)
        if confidence < 0.4:
            bottlenecks.append(Bottleneck(
                edge=edge_key,
                reason=f"Weak link ({confidence:.0%} confidence)",
                severity="medium" if confidence > 0.2 else "high",
                impact_factor=1.5 if confidence > 0.2 else 2.0,
            ))
        
        # Check for high-traffic nodes (if degree data available)
        degree = graph.degree(from_h)
        if degree > 6:
            bottlenecks.append(Bottleneck(
                node=from_h,
                reason=f"High-traffic node ({degree} connections)",
                severity="low",
                impact_factor=1.1,
            ))
    
    return bottlenecks


def _find_historical_paths(
    conn,
    source: str,
    target: str,
    hours: int = 168,
) -> List[AlternativePath]:
    """Find paths that were historically observed."""
    import json
    
    alternatives = []
    cutoff = time.time() - (hours * 3600)
    cursor = conn.cursor()
    
    src_prefix = get_hash_prefix(source)
    dst_prefix = get_hash_prefix(target)
    
    # Search for paths that match source/destination prefixes
    cursor.execute("""
        SELECT path_signature, hop_count, observation_count, last_seen
        FROM path_observations
        WHERE src_hash LIKE ? 
          AND (dst_hash LIKE ? OR dst_hash IS NULL)
          AND last_seen > ?
        ORDER BY observation_count DESC
        LIMIT 5
    """, (f"%{src_prefix}%", f"%{dst_prefix}%", cutoff))
    
    for row in cursor.fetchall():
        try:
            path_sig = row[0]
            path = json.loads(path_sig) if isinstance(path_sig, str) else path_sig
            
            # Convert to full hash format
            full_path = [f"0x{p.upper()}" for p in path]
            
            hop_count = row[1]
            obs_count = row[2]
            
            # Estimate metrics for historical path
            latency, _ = _estimate_latency(hop_count, is_flood=True)
            
            # Success probability based on observation count
            # More observations = more reliable
            base_prob = min(0.9, 0.5 + (obs_count / 100))
            
            alternatives.append(AlternativePath(
                path=full_path,
                hop_count=hop_count,
                estimated_latency_ms=latency,
                success_probability=base_prob,
                is_historical=True,
                observation_count=obs_count,
            ))
        except (json.JSONDecodeError, TypeError):
            continue
    
    return alternatives


def simulate_route(
    conn_or_db: DBConnection,
    source: str,
    target: str,
    via: Optional[str] = None,
    local_hash: Optional[str] = None,
    min_certainty: int = 5,
    hours: int = 168,
) -> RouteSimulation:
    """
    Simulate routing between two nodes.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        source: Source node hash or prefix
        target: Target node hash or prefix
        via: Optional waypoint to route through
        local_hash: Local node's hash
        min_certainty: Minimum certainty for graph edges
        hours: Time range for historical data
        
    Returns:
        RouteSimulation with predicted path and analysis
    """
    source = _normalize_hash(source)
    target = _normalize_hash(target)
    via = _normalize_hash(via) if via else None
    
    result = RouteSimulation(
        source=source,
        target=target,
        via=via,
    )
    
    if not NETWORKX_AVAILABLE:
        result.error = "NetworkX not available"
        return result
    
    with ensure_connection(conn_or_db) as conn:
        # Build graph
        try:
            graph = build_graph_from_db(conn, min_certainty, local_hash)
        except Exception as e:
            result.error = f"Failed to build graph: {e}"
            return result
        
        # Find nodes in graph (may have different hash format)
        src_node = None
        dst_node = None
        via_node = None
        
        src_prefix = get_hash_prefix(source)
        dst_prefix = get_hash_prefix(target)
        via_prefix = get_hash_prefix(via) if via else None
        
        for node in graph.graph.nodes():
            node_prefix = get_hash_prefix(node)
            if node_prefix == src_prefix:
                src_node = node
            if node_prefix == dst_prefix:
                dst_node = node
            if via_prefix and node_prefix == via_prefix:
                via_node = node
        
        if not src_node:
            result.error = f"Source node {src_prefix} not found in network"
            return result
        
        if not dst_node:
            result.error = f"Target node {dst_prefix} not found in network"
            return result
        
        if via and not via_node:
            result.error = f"Waypoint node {via_prefix} not found in network"
            return result
        
        # Build edge confidence lookup
        edge_confidence = {}
        for u, v, data in graph.graph.edges(data=True):
            edge_key = make_edge_key(u, v)
            conf = data.get("avg_confidence", 0.5)
            edge_confidence[edge_key] = conf
        
        # Get critical infrastructure
        articulation_points = set(graph.find_articulation_points())
        bridge_edges = set(graph.find_bridges())
        
        # Find paths
        if via_node:
            # Two-segment path through waypoint
            path1 = graph.shortest_path(src_node, via_node)
            path2 = graph.shortest_path(via_node, dst_node)
            
            if path1 and path2:
                # Combine paths (avoid duplicating via_node)
                combined_path = path1.path + path2.path[1:]
                result.predicted_path = combined_path
                result.hop_count = len(combined_path) - 1
                result.path_found = True
            else:
                result.error = "No path found through waypoint"
                return result
        else:
            # Direct path
            path_result = graph.shortest_path(src_node, dst_node)
            
            if path_result:
                result.predicted_path = path_result.path
                result.hop_count = path_result.hop_count
                result.path_found = True
            else:
                result.error = "No path found between nodes"
                
                # Try to get historical paths anyway
                result.alternative_paths = _find_historical_paths(conn, source, target, hours)
                return result
        
        # Calculate bottleneck factor
        bottlenecks = _detect_bottlenecks(
            graph,
            result.predicted_path,
            edge_confidence,
            articulation_points,
            bridge_edges,
        )
        result.bottlenecks = bottlenecks
        
        bottleneck_factor = 1.0
        for b in bottlenecks:
            bottleneck_factor *= b.impact_factor
        
        # Estimate latency
        latency, latency_range = _estimate_latency(
            result.hop_count,
            is_flood=True,  # Assume flood for safety
            bottleneck_factor=bottleneck_factor,
        )
        result.estimated_latency_ms = latency
        result.estimated_latency_range = latency_range
        
        # Calculate success probability
        result.success_probability = _calculate_success_probability(
            graph,
            result.predicted_path,
            edge_confidence,
        )
        
        # Predict route type based on path length and observation history
        if result.hop_count <= 2:
            result.predicted_route_type = "direct"
        else:
            result.predicted_route_type = "flood"
        
        # Calculate confidence in prediction
        # Based on path observation count and edge confidence
        avg_edge_conf = sum(
            edge_confidence.get(make_edge_key(result.predicted_path[i], result.predicted_path[i+1]), 0.5)
            for i in range(len(result.predicted_path) - 1)
        ) / max(1, len(result.predicted_path) - 1)
        
        result.confidence = avg_edge_conf
        
        # Find alternative paths
        alternatives = []
        
        # Get K shortest from graph
        k_paths = graph.k_shortest_paths(src_node, dst_node, k=4)
        for i, kp in enumerate(k_paths[1:], 1):  # Skip first (it's the predicted path)
            alt_latency, _ = _estimate_latency(kp.hop_count, is_flood=True)
            alt_prob = _calculate_success_probability(graph, kp.path, edge_confidence)
            
            alternatives.append(AlternativePath(
                path=kp.path,
                hop_count=kp.hop_count,
                estimated_latency_ms=alt_latency,
                success_probability=alt_prob,
                is_historical=False,
            ))
        
        # Add historical paths
        historical = _find_historical_paths(conn, source, target, hours)
        for hp in historical:
            # Don't duplicate paths we already have
            hp_sig = ",".join(hp.path)
            if not any(hp_sig == ",".join(a.path) for a in alternatives):
                alternatives.append(hp)
        
        result.alternative_paths = alternatives[:5]  # Top 5
    
    return result


def compare_routes(
    conn_or_db: DBConnection,
    source: str,
    target: str,
    waypoints: List[str],
    **kwargs,
) -> List[RouteSimulation]:
    """
    Compare multiple routes through different waypoints.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        source: Source node hash or prefix
        target: Target node hash or prefix
        waypoints: List of potential waypoints to compare
        **kwargs: Additional arguments passed to simulate_route
        
    Returns:
        List of RouteSimulation for each waypoint, sorted by success probability
    """
    results = []
    
    # Direct route (no waypoint)
    direct = simulate_route(conn_or_db, source, target, **kwargs)
    results.append(direct)
    
    # Routes through each waypoint
    for via in waypoints:
        sim = simulate_route(conn_or_db, source, target, via=via, **kwargs)
        results.append(sim)
    
    # Sort by success probability descending
    results.sort(key=lambda r: r.success_probability, reverse=True)
    
    return results
