"""
Topology - Graph analysis for mesh network visualization
=========================================================

Builds and analyzes the mesh network topology from edge observations.
Computes centrality scores, identifies hub nodes, and detects loops.

This module transforms raw edge observations into a rich topology
model suitable for visualization and network health monitoring.

Key Concepts
------------
    Centrality:
        Measure of how important a node is for routing traffic.
        Computed from edge connectivity and packet counts.
        Score range: 0.0 (peripheral) to 1.0 (most central).
        
    Hub Node:
        A node handling >15% of observed last-hop traffic.
        Hubs are critical infrastructure - if they fail, significant
        portions of the mesh may become unreachable.
        
    Gateway Node:
        A node handling 5-15% of traffic. Important but not critical.
        Often represents regional concentrators or bridge nodes.
        
    Network Loop:
        A cycle in the topology graph, indicating redundant paths.
        Loops provide resilience - traffic can route around failures.
        Detected via DFS up to MAX_LOOP_LENGTH (6) nodes.

Threshold Constants
-------------------
    MIN_EDGE_VALIDATIONS = 15
        Minimum certain observations before edge appears in topology.
        
    HUB_THRESHOLD_PERCENT = 0.15 (15%)
        Traffic share threshold for hub classification.
        
    GATEWAY_THRESHOLD_PERCENT = 0.05 (5%)
        Traffic share threshold for gateway classification.
        
    MAX_LOOP_LENGTH = 6
        Maximum nodes in a detected loop (limits DFS depth).

Topology Snapshot
-----------------
The build_topology_from_db() function produces a TopologySnapshot
containing:

    - edges: List of TopologyEdge with computed metrics
    - hub_nodes: List of hub node hashes
    - gateway_nodes: List of gateway node hashes  
    - centrality: Dict mapping node hash to centrality score
    - loops: List of detected NetworkLoop objects
    - loop_edge_keys: Set of edge keys participating in loops

Caching Strategy
----------------
Topology computation is expensive, so results are cached:

    1. Worker rebuilds topology every 5 minutes
    2. Snapshot saved to topology_snapshot table
    3. API loads cached snapshot (fast)
    4. API can force rebuild with ?rebuild=true

Public Functions
----------------
    build_topology_from_db(conn, local_hash, min_certain_count)
        Build complete topology snapshot from current data.
        
    save_topology_snapshot(conn, snapshot)
        Persist snapshot to database for caching.
        
    load_latest_topology(conn)
        Load most recent cached snapshot.
        
    calculate_centrality(edges, packet_counts)
        Compute centrality scores for all nodes.
        
    detect_hub_nodes(edges, local_hash)
        Identify hub and gateway nodes.
        
    find_network_loops(edges, local_hash, max_length)
        Detect cycles in the topology graph.

API Endpoints
-------------
    GET /api/analytics/topology
        Returns full TopologySnapshot (uses cache by default)
        
    GET /api/analytics/topology?rebuild=true
        Force fresh computation
        
    GET /api/analytics/topology_edges
        Returns just validated edges (lighter response)

See Also
--------
    - edge_builder.py: Produces edge observations consumed here
    - worker.py: Periodically calls build_topology_from_db()
    - analytics_api.py: Exposes topology via REST
"""

import json
import logging
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Union

from .db import AnalyticsDB, ensure_connection, DBConnection
from .edge_builder import get_validated_edges, make_edge_key

# Try to import NetworkX for improved graph algorithms
try:
    from .graph import build_graph_from_db, MeshGraph, NETWORKX_AVAILABLE
except ImportError:
    NETWORKX_AVAILABLE = False
    MeshGraph = None

logger = logging.getLogger("Analytics.Topology")

# Topology thresholds (match frontend constants)
MIN_EDGE_VALIDATIONS = 15
HUB_THRESHOLD_PERCENT = 0.15      # 15% of traffic = hub
GATEWAY_THRESHOLD_PERCENT = 0.05  # 5% of traffic = gateway
MAX_LOOP_LENGTH = 6               # Max nodes in detected loop


@dataclass
class TopologyEdge:
    """Single topology edge with computed metrics."""
    from_hash: str
    to_hash: str
    edge_key: str
    packet_count: int
    certain_count: int
    avg_confidence: float
    forward_count: int
    reverse_count: int
    symmetry_ratio: float
    is_hub_connection: bool = False
    is_loop_edge: bool = False
    is_backbone: bool = False
    strength: float = 0.0
    betweenness: float = 0.0  # Edge betweenness centrality
    
    def to_dict(self) -> dict:
        return {
            "fromHash": self.from_hash,
            "toHash": self.to_hash,
            "edgeKey": self.edge_key,
            "packetCount": self.packet_count,
            "certainCount": self.certain_count,
            "avgConfidence": self.avg_confidence,
            "forwardCount": self.forward_count,
            "reverseCount": self.reverse_count,
            "symmetryRatio": self.symmetry_ratio,
            "isHubConnection": self.is_hub_connection,
            "isLoopEdge": self.is_loop_edge,
            "isBackbone": self.is_backbone,
            "strength": self.strength,
            "betweenness": self.betweenness,
        }


@dataclass
class NetworkLoop:
    """Detected network loop (cycle)."""
    nodes: List[str]
    edge_keys: List[str]
    length: int
    includes_local: bool = False
    
    def to_dict(self) -> dict:
        return {
            "nodes": self.nodes,
            "edgeKeys": self.edge_keys,
            "length": self.length,
            "includesLocal": self.includes_local,
        }


@dataclass
class TopologySnapshot:
    """Complete topology state for API response."""
    computed_at: float
    local_hash: Optional[str]
    edges: List[TopologyEdge] = field(default_factory=list)
    hub_nodes: List[str] = field(default_factory=list)
    gateway_nodes: List[str] = field(default_factory=list)
    centrality: Dict[str, float] = field(default_factory=dict)
    edge_betweenness: Dict[str, float] = field(default_factory=dict)
    backbone_edges: List[str] = field(default_factory=list)
    loops: List[NetworkLoop] = field(default_factory=list)
    loop_edge_keys: Set[str] = field(default_factory=set)
    
    def to_dict(self) -> dict:
        return {
            "computed_at": self.computed_at,
            "local_hash": self.local_hash,
            "edges": [e.to_dict() for e in self.edges],
            "hubNodes": self.hub_nodes,
            "gatewayNodes": self.gateway_nodes,
            "centrality": self.centrality,
            "edgeBetweenness": self.edge_betweenness,
            "backboneEdges": self.backbone_edges,
            "loops": [l.to_dict() for l in self.loops],
            "loopEdgeKeys": list(self.loop_edge_keys),
        }


def build_adjacency_list(edges: List[dict]) -> Dict[str, Set[str]]:
    """
    Build undirected adjacency list from edges.
    
    Args:
        edges: List of edge dicts from database
        
    Returns:
        Dict mapping node hash -> set of adjacent node hashes
    """
    adjacency = defaultdict(set)
    
    for edge in edges:
        from_h = edge.get("fromHash") or edge.get("from_hash")
        to_h = edge.get("toHash") or edge.get("to_hash")
        
        if from_h and to_h:
            adjacency[from_h].add(to_h)
            adjacency[to_h].add(from_h)
    
    return dict(adjacency)


def calculate_edge_betweenness(
    edges: List[dict],
    backbone_threshold: float = 0.1,
    use_networkx: bool = True,
) -> Tuple[Dict[str, float], List[str]]:
    """
    Calculate edge betweenness centrality.
    
    When NetworkX is available, uses optimized algorithm.
    Otherwise falls back to manual BFS implementation.
    
    Edge betweenness measures how many shortest paths pass through each edge.
    High betweenness edges are critical for network connectivity.
    
    Args:
        edges: List of edge dicts
        backbone_threshold: Threshold for backbone classification (default 0.1 = top 10%)
        use_networkx: Whether to use NetworkX if available (default True)
        
    Returns:
        Tuple of (edge_key -> betweenness, list of backbone edge keys)
    """
    # Try NetworkX for optimized edge betweenness
    if use_networkx and NETWORKX_AVAILABLE:
        try:
            import networkx as nx
            
            # Build graph and edge key mapping
            G = nx.DiGraph()
            edge_key_map = {}
            
            for e in edges:
                from_h = e.get("fromHash") or e.get("from_hash")
                to_h = e.get("toHash") or e.get("to_hash")
                key = e.get("edgeKey") or e.get("edge_key")
                if from_h and to_h and key:
                    G.add_edge(from_h, to_h)
                    G.add_edge(to_h, from_h)  # Undirected
                    edge_key_map[(from_h, to_h)] = key
                    edge_key_map[(to_h, from_h)] = key
            
            if G.number_of_edges() == 0:
                return {}, []
            
            # Calculate edge betweenness
            nx_betweenness = nx.edge_betweenness_centrality(G, normalized=True)
            
            # Convert to edge keys
            betweenness = {}
            for (u, v), score in nx_betweenness.items():
                key = edge_key_map.get((u, v))
                if key:
                    betweenness[key] = max(betweenness.get(key, 0), score)
            
            # Identify backbone edges (top 10% by betweenness)
            if betweenness:
                sorted_edges = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)
                threshold_count = max(1, int(len(sorted_edges) * backbone_threshold))
                backbone = [key for key, _ in sorted_edges[:threshold_count]]
            else:
                backbone = []
            
            logger.debug(f"Calculated NetworkX edge betweenness for {len(betweenness)} edges")
            return betweenness, backbone
            
        except Exception as e:
            logger.warning(f"NetworkX edge betweenness failed, falling back to BFS: {e}")
    
    # Fallback: manual BFS implementation (original code)
    from collections import deque
    
    adjacency = build_adjacency_list(edges)
    if not adjacency:
        return {}, []
    
    # Build edge key lookup
    edge_keys = {}
    for e in edges:
        from_h = e.get("fromHash") or e.get("from_hash")
        to_h = e.get("toHash") or e.get("to_hash")
        key = e.get("edgeKey") or e.get("edge_key")
        if from_h and to_h and key:
            edge_keys[(from_h, to_h)] = key
            edge_keys[(to_h, from_h)] = key
    
    # Initialize betweenness scores
    betweenness: Dict[str, float] = {}
    for e in edges:
        key = e.get("edgeKey") or e.get("edge_key")
        if key:
            betweenness[key] = 0.0
    
    nodes = list(adjacency.keys())
    
    # BFS from each node to count shortest paths
    for source in nodes:
        # BFS to find shortest paths from source
        distances: Dict[str, int] = {source: 0}
        predecessors: Dict[str, List[str]] = defaultdict(list)
        num_paths: Dict[str, int] = {source: 1}
        
        queue = deque([source])
        while queue:
            current = queue.popleft()
            for neighbor in adjacency.get(current, set()):
                if neighbor not in distances:
                    # First time visiting this node
                    distances[neighbor] = distances[current] + 1
                    queue.append(neighbor)
                
                # If this is a shortest path, record it
                if distances.get(neighbor, float('inf')) == distances[current] + 1:
                    predecessors[neighbor].append(current)
                    num_paths[neighbor] = num_paths.get(neighbor, 0) + num_paths[current]
        
        # Back-propagate to compute edge contributions
        # Sort nodes by distance (furthest first)
        sorted_nodes = sorted(
            [n for n in distances if n != source],
            key=lambda n: distances[n],
            reverse=True
        )
        
        dependency: Dict[str, float] = defaultdict(float)
        
        for node in sorted_nodes:
            for pred in predecessors[node]:
                # Fraction of shortest paths through this predecessor
                frac = num_paths[pred] / num_paths[node]
                
                # Edge contribution
                edge_key = edge_keys.get((pred, node))
                if edge_key:
                    contribution = frac * (1 + dependency[node])
                    betweenness[edge_key] = betweenness.get(edge_key, 0) + contribution
                    dependency[pred] += frac * (1 + dependency[node])
    
    # Normalize by number of node pairs
    n = len(nodes)
    if n > 1:
        normalization = 2.0 / ((n - 1) * (n - 2)) if n > 2 else 1.0
        for key in betweenness:
            betweenness[key] *= normalization
    
    # Identify backbone edges (top 10% by betweenness)
    if betweenness:
        sorted_edges = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)
        threshold_count = max(1, int(len(sorted_edges) * backbone_threshold))
        backbone = [key for key, _ in sorted_edges[:threshold_count]]
    else:
        backbone = []
    
    return betweenness, backbone


def calculate_centrality(
    edges: List[dict],
    packet_counts: Optional[Dict[str, int]] = None,
    use_networkx: bool = True,
) -> Dict[str, float]:
    """
    Calculate betweenness centrality for all nodes.
    
    When NetworkX is available, uses proper betweenness centrality algorithm.
    Otherwise falls back to simplified edge-count based approach.
    
    Args:
        edges: List of edge dicts
        packet_counts: Optional node -> packet count for weighting
        use_networkx: Whether to use NetworkX if available (default True)
        
    Returns:
        Dict mapping node hash -> centrality score (0-1)
    """
    # Try NetworkX for proper betweenness centrality
    if use_networkx and NETWORKX_AVAILABLE and MeshGraph is not None:
        try:
            import networkx as nx
            
            # Build graph from edges
            G = nx.DiGraph()
            for e in edges:
                from_h = e.get("fromHash") or e.get("from_hash")
                to_h = e.get("toHash") or e.get("to_hash")
                weight = e.get("certainCount") or e.get("certain_count", 1)
                if from_h and to_h:
                    G.add_edge(from_h, to_h, weight=weight)
                    G.add_edge(to_h, from_h, weight=weight)  # Undirected
            
            if G.number_of_nodes() == 0:
                return {}
            
            # Calculate betweenness centrality
            centrality = nx.betweenness_centrality(G, normalized=True)
            
            logger.debug(f"Calculated NetworkX betweenness centrality for {len(centrality)} nodes")
            return centrality
            
        except Exception as e:
            logger.warning(f"NetworkX centrality failed, falling back to simple method: {e}")
    
    # Fallback: simplified approach based on edge connectivity
    adjacency = build_adjacency_list(edges)
    
    if not adjacency:
        return {}
    
    # Count edges per node
    edge_counts = {}
    for node, neighbors in adjacency.items():
        edge_counts[node] = len(neighbors)
    
    max_edges = max(edge_counts.values()) if edge_counts else 1
    
    # Calculate centrality
    centrality = {}
    for node, count in edge_counts.items():
        # Base centrality from edge count
        base_score = count / max_edges
        
        # Weight by packet count if available
        if packet_counts and node in packet_counts:
            max_packets = max(packet_counts.values()) if packet_counts else 1
            packet_score = packet_counts[node] / max_packets
            centrality[node] = (base_score * 0.6) + (packet_score * 0.4)
        else:
            centrality[node] = base_score
    
    return centrality


def detect_hub_nodes(
    edges: List[dict],
    local_hash: Optional[str] = None,
) -> Tuple[List[str], List[str]]:
    """
    Identify hub and gateway nodes based on traffic patterns.
    
    Hub: >15% of last-hop traffic to local
    Gateway: 5-15% of last-hop traffic
    
    Args:
        edges: List of edge dicts
        local_hash: Local node's hash (for filtering)
        
    Returns:
        Tuple of (hub_nodes, gateway_nodes) lists
    """
    # Count traffic per node
    traffic_counts = defaultdict(int)
    total_traffic = 0
    
    for edge in edges:
        from_h = edge.get("fromHash") or edge.get("from_hash")
        to_h = edge.get("toHash") or edge.get("to_hash")
        count = edge.get("certainCount") or edge.get("certain_count", 0)
        
        # Count traffic for both endpoints
        if from_h and from_h != local_hash:
            traffic_counts[from_h] += count
            total_traffic += count
        if to_h and to_h != local_hash:
            traffic_counts[to_h] += count
            total_traffic += count
    
    if total_traffic == 0:
        return [], []
    
    hubs = []
    gateways = []
    
    for node, count in traffic_counts.items():
        traffic_percent = count / total_traffic
        
        if traffic_percent >= HUB_THRESHOLD_PERCENT:
            hubs.append(node)
        elif traffic_percent >= GATEWAY_THRESHOLD_PERCENT:
            gateways.append(node)
    
    return hubs, gateways


def find_network_loops(
    edges: List[dict],
    local_hash: Optional[str] = None,
    max_length: int = MAX_LOOP_LENGTH,
) -> Tuple[List[NetworkLoop], Set[str]]:
    """
    Detect network loops (cycles) in the topology.
    
    Uses DFS to find cycles up to max_length nodes.
    
    Args:
        edges: List of edge dicts
        local_hash: Local node's hash
        max_length: Maximum loop length to detect
        
    Returns:
        Tuple of (list of NetworkLoop, set of edge keys in loops)
    """
    adjacency = build_adjacency_list(edges)
    
    if not adjacency:
        return [], set()
    
    loops = []
    loop_edge_keys = set()
    visited_cycles = set()  # Track found cycles to avoid duplicates
    
    def dfs_find_cycles(start: str, current: str, path: List[str], depth: int):
        if depth > max_length:
            return
        
        for neighbor in adjacency.get(current, []):
            if neighbor == start and len(path) >= 3:
                # Found a cycle
                cycle_key = tuple(sorted(path))
                if cycle_key not in visited_cycles:
                    visited_cycles.add(cycle_key)
                    
                    # Build edge keys for this loop
                    edge_keys = []
                    for i in range(len(path)):
                        from_node = path[i]
                        to_node = path[(i + 1) % len(path)]
                        edge_keys.append(make_edge_key(from_node, to_node))
                    
                    includes_local = local_hash in path if local_hash else False
                    
                    loops.append(NetworkLoop(
                        nodes=list(path),
                        edge_keys=edge_keys,
                        length=len(path),
                        includes_local=includes_local,
                    ))
                    
                    loop_edge_keys.update(edge_keys)
                    
            elif neighbor not in path:
                dfs_find_cycles(start, neighbor, path + [neighbor], depth + 1)
    
    # Start DFS from each node
    for start_node in adjacency.keys():
        dfs_find_cycles(start_node, start_node, [start_node], 1)
    
    return loops, loop_edge_keys


def build_topology_from_db(
    conn_or_db: DBConnection,
    local_hash: Optional[str] = None,
    min_certain_count: int = MIN_EDGE_VALIDATIONS,
) -> TopologySnapshot:
    """
    Build complete topology snapshot from database.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        local_hash: Local node's hash
        min_certain_count: Minimum certain observations for edge validation
        
    Returns:
        TopologySnapshot with all computed data
    """
    computed_at = time.time()
    
    # Get validated edges from database
    with ensure_connection(conn_or_db) as conn:
        edge_dicts = get_validated_edges(conn, min_certain_count=min_certain_count)
    
    if not edge_dicts:
        return TopologySnapshot(
            computed_at=computed_at,
            local_hash=local_hash,
        )
    
    # Calculate centrality
    centrality = calculate_centrality(edge_dicts)
    
    # Calculate edge betweenness and identify backbone
    edge_betweenness, backbone_edges = calculate_edge_betweenness(edge_dicts)
    backbone_set = set(backbone_edges)
    
    # Detect hubs and gateways
    hub_nodes, gateway_nodes = detect_hub_nodes(edge_dicts, local_hash)
    hub_set = set(hub_nodes)
    
    # Detect loops
    loops, loop_edge_keys = find_network_loops(edge_dicts, local_hash)
    
    # Find max packet count for strength calculation
    max_packets = max(
        (e.get("packetCount") or e.get("packet_count", 0) for e in edge_dicts),
        default=1
    )
    
    # Convert to TopologyEdge objects
    edges = []
    for e in edge_dicts:
        from_h = e.get("fromHash") or e.get("from_hash")
        to_h = e.get("toHash") or e.get("to_hash")
        edge_key = e.get("edgeKey") or e.get("edge_key")
        packet_count = e.get("packetCount") or e.get("packet_count", 0)
        avg_conf = e.get("avgConfidence") or e.get("avg_confidence", 0)
        
        # Calculate strength
        normalized_count = packet_count / max_packets if max_packets > 0 else 0
        strength = (normalized_count * 0.5) + (avg_conf * 0.5)
        
        edge = TopologyEdge(
            from_hash=from_h,
            to_hash=to_h,
            edge_key=edge_key,
            packet_count=packet_count,
            certain_count=e.get("certainCount") or e.get("certain_count", 0),
            avg_confidence=avg_conf,
            forward_count=e.get("forwardCount") or e.get("forward_count", 0),
            reverse_count=e.get("reverseCount") or e.get("reverse_count", 0),
            symmetry_ratio=e.get("symmetryRatio") or e.get("symmetry_ratio", 0),
            is_hub_connection=(from_h in hub_set or to_h in hub_set),
            is_loop_edge=(edge_key in loop_edge_keys),
            is_backbone=(edge_key in backbone_set),
            strength=round(strength, 3),
            betweenness=round(edge_betweenness.get(edge_key, 0), 4),
        )
        edges.append(edge)
    
    # Sort by certain count descending
    edges.sort(key=lambda x: x.certain_count, reverse=True)
    
    return TopologySnapshot(
        computed_at=computed_at,
        local_hash=local_hash,
        edges=edges,
        hub_nodes=hub_nodes,
        gateway_nodes=gateway_nodes,
        centrality={k: round(v, 3) for k, v in centrality.items()},
        edge_betweenness={k: round(v, 4) for k, v in edge_betweenness.items()},
        backbone_edges=backbone_edges,
        loops=loops,
        loop_edge_keys=loop_edge_keys,
    )


def save_topology_snapshot(conn_or_db: DBConnection, snapshot: TopologySnapshot):
    """
    Save topology snapshot to database.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        snapshot: TopologySnapshot to save
    """
    with ensure_connection(conn_or_db) as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO topology_snapshot (
                computed_at, local_hash, edges_json, hub_nodes_json,
                centrality_json, loops_json, stats_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot.computed_at,
            snapshot.local_hash,
            json.dumps([e.to_dict() for e in snapshot.edges]),
            json.dumps(snapshot.hub_nodes),
            json.dumps(snapshot.centrality),
            json.dumps([l.to_dict() for l in snapshot.loops]),
            json.dumps({"gateway_nodes": snapshot.gateway_nodes}),
        ))
        
        logger.info(f"Saved topology snapshot with {len(snapshot.edges)} edges")


def load_latest_topology(conn_or_db: DBConnection) -> Optional[TopologySnapshot]:
    """
    Load most recent topology snapshot from database.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        
    Returns:
        TopologySnapshot or None if not found
    """
    with ensure_connection(conn_or_db) as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT computed_at, local_hash, edges_json, hub_nodes_json,
                   centrality_json, loops_json, stats_json
            FROM topology_snapshot
            ORDER BY computed_at DESC
            LIMIT 1
        """)
        
        row = cursor.fetchone()
        if not row:
            return None
        
        try:
            edges_data = json.loads(row[2]) if row[2] else []
            edges = []
            for e in edges_data:
                edges.append(TopologyEdge(
                    from_hash=e.get("fromHash", ""),
                    to_hash=e.get("toHash", ""),
                    edge_key=e.get("edgeKey", ""),
                    packet_count=e.get("packetCount", 0),
                    certain_count=e.get("certainCount", 0),
                    avg_confidence=e.get("avgConfidence", 0),
                    forward_count=e.get("forwardCount", 0),
                    reverse_count=e.get("reverseCount", 0),
                    symmetry_ratio=e.get("symmetryRatio", 0),
                    is_hub_connection=e.get("isHubConnection", False),
                    is_loop_edge=e.get("isLoopEdge", False),
                    strength=e.get("strength", 0),
                ))
            
            loops_data = json.loads(row[5]) if row[5] else []
            loops = []
            loop_edge_keys = set()
            for l in loops_data:
                loop = NetworkLoop(
                    nodes=l.get("nodes", []),
                    edge_keys=l.get("edgeKeys", []),
                    length=l.get("length", 0),
                    includes_local=l.get("includesLocal", False),
                )
                loops.append(loop)
                loop_edge_keys.update(loop.edge_keys)
            
            stats = json.loads(row[6]) if row[6] else {}
            
            return TopologySnapshot(
                computed_at=row[0],
                local_hash=row[1],
                edges=edges,
                hub_nodes=json.loads(row[3]) if row[3] else [],
                gateway_nodes=stats.get("gateway_nodes", []),
                centrality=json.loads(row[4]) if row[4] else {},
                loops=loops,
                loop_edge_keys=loop_edge_keys,
            )
            
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse topology snapshot: {e}")
            return None
