"""
Graph Analysis - NetworkX-powered mesh topology analysis
=========================================================

Provides in-memory graph analysis using NetworkX, backed by SQLite
for persistent edge observations. This module enables powerful graph
algorithms without requiring an external graph database.

Architecture
------------
    SQLite (persistence) --> NetworkX (in-memory analysis)
    
    1. Edge observations stored in SQLite (edge_observations table)
    2. On-demand graph construction from SQLite data
    3. NetworkX algorithms for path finding, centrality, etc.
    4. Results returned directly or cached in topology_snapshot

Key Features
------------
    - Build directed or undirected graphs from edge observations
    - Shortest path finding between any two nodes
    - All simple paths enumeration (with cutoff)
    - Betweenness centrality (node and edge)
    - PageRank for node importance
    - Connected component detection
    - Cycle/loop detection
    - Network diameter and radius

Caching Strategy
----------------
    The MeshGraph class maintains a cached NetworkX graph that can be
    rebuilt on demand. For API queries, the graph is typically rebuilt
    fresh from current data. For expensive computations, results are
    stored in topology_snapshot.

Public Classes
--------------
    MeshGraph
        Main class for graph operations. Wraps NetworkX DiGraph with
        mesh-specific methods.

Public Functions
----------------
    build_graph_from_db(conn_or_db, min_certain_count)
        Build MeshGraph from edge_observations table.
        
    get_shortest_path(graph, source, target)
        Find shortest path between two nodes.
        
    get_all_paths(graph, source, target, cutoff)
        Find all simple paths up to cutoff length.
        
    get_node_centrality(graph, algorithm)
        Calculate node centrality using various algorithms.

Example
-------
    >>> from repeater.analytics.graph import build_graph_from_db
    >>> 
    >>> with db.connection() as conn:
    ...     graph = build_graph_from_db(conn)
    ...     
    >>> # Find path between nodes
    >>> path = graph.shortest_path('AB', 'EF')
    >>> print(path)  # ['AB', 'CD', 'EF']
    >>> 
    >>> # Get most important relay nodes
    >>> centrality = graph.betweenness_centrality()
    >>> print(centrality)  # {'CD': 0.85, 'AB': 0.42, ...}

See Also
--------
    - topology.py: Uses MeshGraph for topology computation
    - analytics_api.py: Exposes graph queries via REST
    - edge_builder.py: Populates edge_observations table
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Union

try:
    import networkx as nx
    NETWORKX_AVAILABLE = True
except ImportError:
    NETWORKX_AVAILABLE = False
    nx = None

from .db import AnalyticsDB, ensure_connection, DBConnection

logger = logging.getLogger("Analytics.Graph")

# Default minimum certainty for including edges
DEFAULT_MIN_CERTAIN_COUNT = 5


@dataclass
class PathResult:
    """Result of a path query."""
    source: str
    target: str
    path: List[str]
    hop_count: int
    total_weight: float = 0.0
    edge_keys: List[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "target": self.target,
            "path": self.path,
            "hopCount": self.hop_count,
            "totalWeight": round(self.total_weight, 3),
            "edgeKeys": self.edge_keys,
        }


@dataclass
class CentralityResult:
    """Result of centrality computation."""
    algorithm: str
    scores: Dict[str, float]
    computed_at: float
    top_nodes: List[Tuple[str, float]] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            "algorithm": self.algorithm,
            "scores": {k: round(v, 4) for k, v in self.scores.items()},
            "computedAt": self.computed_at,
            "topNodes": [
                {"node": node, "score": round(score, 4)} 
                for node, score in self.top_nodes
            ],
        }


@dataclass 
class ComponentResult:
    """Result of connected components analysis."""
    total_nodes: int
    total_components: int
    largest_component_size: int
    components: List[List[str]]
    isolated_nodes: List[str]
    
    def to_dict(self) -> dict:
        return {
            "totalNodes": self.total_nodes,
            "totalComponents": self.total_components,
            "largestComponentSize": self.largest_component_size,
            "components": self.components,
            "isolatedNodes": self.isolated_nodes,
        }


@dataclass
class MaxFlowEdge:
    """A single edge in the max flow solution."""
    from_node: str
    to_node: str
    flow: float
    capacity: float
    
    def to_dict(self) -> dict:
        return {
            "from": self.from_node,
            "to": self.to_node,
            "flow": round(self.flow, 2),
            "capacity": round(self.capacity, 2),
        }


@dataclass
class MaxFlowResult:
    """
    Result of maximum flow calculation.
    
    Contains the max flow value plus detailed flow on each edge,
    matching the frontend MaxFlowResponse interface.
    """
    source: str
    target: str
    max_flow_value: float
    flow_edges: List[MaxFlowEdge] = field(default_factory=list)
    path_exists: bool = True
    
    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "target": self.target,
            "maxFlowValue": round(self.max_flow_value, 2),
            "flowEdges": [e.to_dict() for e in self.flow_edges],
            "pathExists": self.path_exists,
        }


class MeshGraph:
    """
    NetworkX-backed mesh network graph.
    
    Provides mesh-specific graph operations built on top of NetworkX.
    Can be constructed from SQLite edge observations or built manually.
    
    Attributes:
        graph: Underlying NetworkX DiGraph
        local_hash: Local node's hash (optional)
        built_at: Timestamp when graph was constructed
        
    Example:
        >>> mesh = MeshGraph()
        >>> mesh.add_edge('AB', 'CD', weight=100, confidence=0.95)
        >>> mesh.add_edge('CD', 'EF', weight=50, confidence=0.80)
        >>> mesh.shortest_path('AB', 'EF')
        ['AB', 'CD', 'EF']
    """
    
    def __init__(self, local_hash: Optional[str] = None):
        """
        Initialize empty mesh graph.
        
        Args:
            local_hash: Local node's hash for special handling
        """
        if not NETWORKX_AVAILABLE:
            raise ImportError(
                "NetworkX is required for graph analysis. "
                "Install with: pip install networkx>=3.0"
            )
        
        self.graph: nx.DiGraph = nx.DiGraph()
        self.local_hash = local_hash
        self.built_at = time.time()
        self._edge_key_map: Dict[Tuple[str, str], str] = {}
    
    @property
    def node_count(self) -> int:
        """Number of nodes in the graph."""
        return self.graph.number_of_nodes()
    
    @property
    def edge_count(self) -> int:
        """Number of edges in the graph."""
        return self.graph.number_of_edges()
    
    def add_node(self, node_hash: str, **attrs) -> None:
        """Add a node with optional attributes."""
        self.graph.add_node(node_hash, **attrs)
    
    def add_edge(
        self,
        from_hash: str,
        to_hash: str,
        edge_key: Optional[str] = None,
        weight: float = 1.0,
        **attrs
    ) -> None:
        """
        Add an edge between two nodes.
        
        Args:
            from_hash: Source node hash
            to_hash: Target node hash  
            edge_key: Canonical edge key (auto-generated if not provided)
            weight: Edge weight (higher = more traffic/stronger link)
            **attrs: Additional edge attributes
        """
        # Store edge key mapping
        if edge_key:
            self._edge_key_map[(from_hash, to_hash)] = edge_key
            self._edge_key_map[(to_hash, from_hash)] = edge_key
        
        self.graph.add_edge(from_hash, to_hash, weight=weight, **attrs)
        
        # For undirected analysis, also add reverse edge
        if not self.graph.has_edge(to_hash, from_hash):
            self.graph.add_edge(to_hash, from_hash, weight=weight, **attrs)
    
    def get_edge_key(self, from_hash: str, to_hash: str) -> Optional[str]:
        """Get canonical edge key for a node pair."""
        return self._edge_key_map.get((from_hash, to_hash))
    
    def has_node(self, node_hash: str) -> bool:
        """Check if node exists in graph."""
        return self.graph.has_node(node_hash)
    
    def has_edge(self, from_hash: str, to_hash: str) -> bool:
        """Check if edge exists between nodes."""
        return self.graph.has_edge(from_hash, to_hash)
    
    def neighbors(self, node_hash: str) -> List[str]:
        """Get all neighbors of a node (both directions)."""
        if not self.has_node(node_hash):
            return []
        
        # Combine predecessors and successors for full neighbor list
        preds = set(self.graph.predecessors(node_hash))
        succs = set(self.graph.successors(node_hash))
        return list(preds | succs)
    
    def degree(self, node_hash: str) -> int:
        """Get degree (number of connections) for a node."""
        if not self.has_node(node_hash):
            return 0
        return len(self.neighbors(node_hash))
    
    # === PATH FINDING ===
    
    def shortest_path(
        self,
        source: str,
        target: str,
        weight: Optional[str] = None,
    ) -> Optional[PathResult]:
        """
        Find shortest path between two nodes.
        
        Args:
            source: Source node hash
            target: Target node hash
            weight: Edge attribute to use as weight (None = hop count)
            
        Returns:
            PathResult or None if no path exists
        """
        if not self.has_node(source) or not self.has_node(target):
            return None
        
        try:
            if weight:
                # Use weighted shortest path (Dijkstra)
                path = nx.dijkstra_path(self.graph, source, target, weight=weight)
                length = nx.dijkstra_path_length(self.graph, source, target, weight=weight)
            else:
                # Use unweighted shortest path (BFS)
                path = nx.shortest_path(self.graph, source, target)
                length = len(path) - 1
            
            # Build edge keys for the path
            edge_keys = []
            for i in range(len(path) - 1):
                key = self.get_edge_key(path[i], path[i + 1])
                if key:
                    edge_keys.append(key)
            
            return PathResult(
                source=source,
                target=target,
                path=path,
                hop_count=len(path) - 1,
                total_weight=length if weight else 0,
                edge_keys=edge_keys,
            )
            
        except nx.NetworkXNoPath:
            return None
        except nx.NodeNotFound:
            return None
    
    def all_paths(
        self,
        source: str,
        target: str,
        cutoff: int = 6,
    ) -> List[PathResult]:
        """
        Find all simple paths between two nodes.
        
        Args:
            source: Source node hash
            target: Target node hash
            cutoff: Maximum path length (default 6 to limit computation)
            
        Returns:
            List of PathResult objects, sorted by hop count
        """
        if not self.has_node(source) or not self.has_node(target):
            return []
        
        results = []
        
        try:
            for path in nx.all_simple_paths(self.graph, source, target, cutoff=cutoff):
                edge_keys = []
                for i in range(len(path) - 1):
                    key = self.get_edge_key(path[i], path[i + 1])
                    if key:
                        edge_keys.append(key)
                
                results.append(PathResult(
                    source=source,
                    target=target,
                    path=path,
                    hop_count=len(path) - 1,
                    edge_keys=edge_keys,
                ))
        except nx.NodeNotFound:
            pass
        
        # Sort by hop count
        results.sort(key=lambda r: r.hop_count)
        return results
    
    def path_exists(self, source: str, target: str) -> bool:
        """Check if a path exists between two nodes."""
        if not self.has_node(source) or not self.has_node(target):
            return False
        return nx.has_path(self.graph, source, target)
    
    # === CENTRALITY ===
    
    def betweenness_centrality(self, normalized: bool = True) -> CentralityResult:
        """
        Calculate betweenness centrality for all nodes.
        
        Betweenness measures how often a node lies on shortest paths
        between other nodes. High betweenness = important relay node.
        
        Args:
            normalized: Whether to normalize scores to [0, 1]
            
        Returns:
            CentralityResult with scores for all nodes
        """
        scores = nx.betweenness_centrality(self.graph, normalized=normalized)
        
        # Sort by score descending
        sorted_nodes = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        return CentralityResult(
            algorithm="betweenness",
            scores=scores,
            computed_at=time.time(),
            top_nodes=sorted_nodes[:10],
        )
    
    def degree_centrality(self) -> CentralityResult:
        """
        Calculate degree centrality for all nodes.
        
        Degree centrality = number of connections / (n-1).
        Simple measure of how well-connected a node is.
        
        Returns:
            CentralityResult with scores for all nodes
        """
        scores = nx.degree_centrality(self.graph)
        sorted_nodes = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        return CentralityResult(
            algorithm="degree",
            scores=scores,
            computed_at=time.time(),
            top_nodes=sorted_nodes[:10],
        )
    
    def pagerank(self, alpha: float = 0.85) -> CentralityResult:
        """
        Calculate PageRank for all nodes.
        
        PageRank models random walk importance - nodes that are
        linked to by important nodes get higher scores.
        
        Note: Requires numpy. Falls back to degree centrality if numpy
        is not available.
        
        Args:
            alpha: Damping factor (default 0.85)
            
        Returns:
            CentralityResult with scores for all nodes
        """
        try:
            scores = nx.pagerank(self.graph, alpha=alpha)
        except nx.PowerIterationFailedConvergence:
            # Fall back to simpler method if PageRank doesn't converge
            logger.warning("PageRank failed to converge, using degree centrality")
            return self.degree_centrality()
        except ImportError:
            # numpy not available, fall back to degree centrality
            logger.warning("PageRank requires numpy, using degree centrality instead")
            return self.degree_centrality()
        except Exception as e:
            logger.warning(f"PageRank failed ({e}), using degree centrality")
            return self.degree_centrality()
        
        sorted_nodes = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        return CentralityResult(
            algorithm="pagerank",
            scores=scores,
            computed_at=time.time(),
            top_nodes=sorted_nodes[:10],
        )
    
    def closeness_centrality(self) -> CentralityResult:
        """
        Calculate closeness centrality for all nodes.
        
        Closeness = 1 / (average shortest path length to all other nodes).
        High closeness = can reach other nodes quickly.
        
        Returns:
            CentralityResult with scores for all nodes
        """
        scores = nx.closeness_centrality(self.graph)
        sorted_nodes = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        return CentralityResult(
            algorithm="closeness",
            scores=scores,
            computed_at=time.time(),
            top_nodes=sorted_nodes[:10],
        )
    
    def edge_betweenness_centrality(self, normalized: bool = True) -> Dict[str, float]:
        """
        Calculate betweenness centrality for edges.
        
        Returns:
            Dict mapping edge key -> betweenness score
        """
        edge_scores = nx.edge_betweenness_centrality(self.graph, normalized=normalized)
        
        # Convert to edge keys
        result = {}
        for (u, v), score in edge_scores.items():
            key = self.get_edge_key(u, v)
            if key:
                # Take max of both directions
                result[key] = max(result.get(key, 0), score)
        
        return result
    
    # === COMPONENT ANALYSIS ===
    
    def connected_components(self) -> ComponentResult:
        """
        Find connected components in the graph.
        
        Uses weakly connected components since this is a directed graph.
        
        Returns:
            ComponentResult with component details
        """
        # Get weakly connected components (ignores edge direction)
        components = list(nx.weakly_connected_components(self.graph))
        
        # Sort by size descending
        components = sorted(components, key=len, reverse=True)
        
        # Convert sets to lists
        component_lists = [list(c) for c in components]
        
        # Find isolated nodes (components of size 1)
        isolated = [c[0] for c in component_lists if len(c) == 1]
        
        return ComponentResult(
            total_nodes=self.node_count,
            total_components=len(components),
            largest_component_size=len(components[0]) if components else 0,
            components=component_lists,
            isolated_nodes=isolated,
        )
    
    def is_connected(self) -> bool:
        """Check if the graph is fully connected."""
        if self.node_count == 0:
            return True
        return nx.is_weakly_connected(self.graph)
    
    # === CYCLE DETECTION ===
    
    def find_cycles(self, max_length: int = 6) -> List[List[str]]:
        """
        Find all cycles (loops) in the graph.
        
        Args:
            max_length: Maximum cycle length to detect
            
        Returns:
            List of cycles (each cycle is a list of node hashes)
        """
        cycles = []
        
        try:
            # Use simple_cycles for directed graph
            for cycle in nx.simple_cycles(self.graph):
                if len(cycle) <= max_length:
                    cycles.append(cycle)
        except nx.NetworkXError:
            pass
        
        # Sort by length
        cycles.sort(key=len)
        return cycles
    
    # === NETWORK METRICS ===
    
    def diameter(self) -> Optional[int]:
        """
        Calculate network diameter (longest shortest path).
        
        Returns:
            Diameter or None if graph is not connected
        """
        if not self.is_connected() or self.node_count == 0:
            return None
        
        try:
            # Convert to undirected for diameter calculation
            undirected = self.graph.to_undirected()
            return nx.diameter(undirected)
        except nx.NetworkXError:
            return None
    
    def radius(self) -> Optional[int]:
        """
        Calculate network radius (minimum eccentricity).
        
        Returns:
            Radius or None if graph is not connected
        """
        if not self.is_connected() or self.node_count == 0:
            return None
        
        try:
            undirected = self.graph.to_undirected()
            return nx.radius(undirected)
        except nx.NetworkXError:
            return None
    
    def average_path_length(self) -> Optional[float]:
        """
        Calculate average shortest path length.
        
        Returns:
            Average path length or None if graph is not connected
        """
        if not self.is_connected() or self.node_count < 2:
            return None
        
        try:
            undirected = self.graph.to_undirected()
            return nx.average_shortest_path_length(undirected)
        except nx.NetworkXError:
            return None
    
    def clustering_coefficient(self) -> float:
        """
        Calculate average clustering coefficient.
        
        Measures how much nodes tend to cluster together.
        
        Returns:
            Average clustering coefficient [0, 1]
        """
        if self.node_count == 0:
            return 0.0
        
        undirected = self.graph.to_undirected()
        return nx.average_clustering(undirected)
    
    def density(self) -> float:
        """
        Calculate graph density.
        
        Density = actual edges / possible edges.
        
        Returns:
            Density [0, 1]
        """
        return nx.density(self.graph)
    
    # === ADVANCED PATH FINDING ===
    
    def k_shortest_paths(
        self,
        source: str,
        target: str,
        k: int = 5,
    ) -> List[PathResult]:
        """
        Find K shortest paths between two nodes using Yen's algorithm.
        
        Unlike all_paths(), this finds paths in order of length/weight,
        guaranteed to return the K best paths.
        
        Args:
            source: Source node hash
            target: Target node hash
            k: Number of paths to find (default 5)
            
        Returns:
            List of PathResult objects, sorted by hop count
        """
        if not self.has_node(source) or not self.has_node(target):
            return []
        
        results = []
        
        try:
            # Use NetworkX's shortest_simple_paths (Yen's algorithm)
            for i, path in enumerate(nx.shortest_simple_paths(self.graph, source, target)):
                if i >= k:
                    break
                
                edge_keys = []
                for j in range(len(path) - 1):
                    key = self.get_edge_key(path[j], path[j + 1])
                    if key:
                        edge_keys.append(key)
                
                results.append(PathResult(
                    source=source,
                    target=target,
                    path=path,
                    hop_count=len(path) - 1,
                    edge_keys=edge_keys,
                ))
        except nx.NetworkXNoPath:
            pass
        except nx.NodeNotFound:
            pass
        
        return results
    
    # === CRITICAL INFRASTRUCTURE DETECTION ===
    
    def find_bridges(self) -> List[Tuple[str, str]]:
        """
        Find bridge edges (single points of failure).
        
        A bridge is an edge whose removal disconnects the graph.
        These are critical links that should have redundant paths.
        
        Returns:
            List of (from_hash, to_hash) tuples for bridge edges
        """
        if self.node_count == 0:
            return []
        
        # Convert to undirected for bridge detection
        undirected = self.graph.to_undirected()
        
        try:
            bridges = list(nx.bridges(undirected))
            return bridges
        except nx.NetworkXError:
            return []
    
    def find_articulation_points(self) -> List[str]:
        """
        Find articulation points (critical nodes).
        
        An articulation point is a node whose removal disconnects the graph.
        These are critical infrastructure nodes that need redundancy.
        
        Returns:
            List of node hashes that are articulation points
        """
        if self.node_count == 0:
            return []
        
        # Convert to undirected for articulation point detection
        undirected = self.graph.to_undirected()
        
        try:
            return list(nx.articulation_points(undirected))
        except nx.NetworkXError:
            return []
    
    # === COMMUNITY DETECTION ===
    
    def detect_communities(self) -> List[List[str]]:
        """
        Detect network communities using greedy modularity.
        
        Communities are groups of nodes that are more densely connected
        to each other than to the rest of the network.
        
        Returns:
            List of communities, each community is a list of node hashes
        """
        if self.node_count < 2:
            return [list(self.graph.nodes())] if self.node_count == 1 else []
        
        # Convert to undirected for community detection
        undirected = self.graph.to_undirected()
        
        try:
            # Use greedy modularity communities
            communities = nx.community.greedy_modularity_communities(undirected)
            return [list(c) for c in communities]
        except Exception:
            # Fallback: return connected components as communities
            return [list(c) for c in nx.connected_components(undirected)]
    
    # === SPANNING TREE ===
    
    def minimum_spanning_tree(self) -> List[Tuple[str, str, dict]]:
        """
        Find minimum spanning tree (backbone network).
        
        The MST connects all nodes with minimum total edge weight.
        Useful for identifying the core backbone structure.
        
        Returns:
            List of (from_hash, to_hash, edge_data) tuples in the MST
        """
        if self.node_count < 2:
            return []
        
        # Convert to undirected for MST
        undirected = self.graph.to_undirected()
        
        try:
            # Use inverse weight so higher traffic = lower cost
            mst = nx.minimum_spanning_tree(undirected, weight='weight')
            return list(mst.edges(data=True))
        except nx.NetworkXError:
            return []
    
    # === NETWORK RESILIENCE ===
    
    def resilience_score(self) -> float:
        """
        Calculate network resilience score (0-1).
        
        Resilience measures how well the network can handle failures.
        Based on:
        - Connectivity (are all nodes reachable?)
        - Redundancy (multiple paths between nodes)
        - Critical infrastructure (bridges and articulation points)
        
        Returns:
            Resilience score from 0 (fragile) to 1 (highly resilient)
        """
        if self.node_count < 2:
            return 0.0 if self.node_count == 0 else 1.0
        
        scores = []
        
        # Factor 1: Connectivity (40%)
        components = self.connected_components()
        if components.total_components == 1:
            connectivity_score = 1.0
        else:
            # Penalize for fragmentation
            largest_ratio = components.largest_component_size / components.total_nodes
            connectivity_score = largest_ratio * 0.5  # Max 0.5 if fragmented
        scores.append(connectivity_score * 0.4)
        
        # Factor 2: Redundancy - ratio of edges to minimum needed (30%)
        # MST needs (n-1) edges, more edges = more redundancy
        min_edges = self.node_count - 1
        actual_edges = self.edge_count // 2  # Divide by 2 for undirected
        if min_edges > 0:
            redundancy_ratio = min(2.0, actual_edges / min_edges)  # Cap at 2x
            redundancy_score = (redundancy_ratio - 1.0) / 1.0  # 0 at 1x, 1 at 2x
            redundancy_score = max(0, min(1, redundancy_score))
        else:
            redundancy_score = 0.0
        scores.append(redundancy_score * 0.3)
        
        # Factor 3: Critical infrastructure ratio (30%)
        bridges = self.find_bridges()
        articulation_pts = self.find_articulation_points()
        
        # Fewer critical points = better resilience
        total_edges = max(1, self.edge_count // 2)
        bridge_ratio = 1 - (len(bridges) / total_edges)
        
        total_nodes = max(1, self.node_count)
        articulation_ratio = 1 - (len(articulation_pts) / total_nodes)
        
        critical_score = (bridge_ratio + articulation_ratio) / 2
        scores.append(critical_score * 0.3)
        
        return round(sum(scores), 3)
    
    # === NETWORK FLOW ===
    
    def max_flow(self, source: str, target: str) -> MaxFlowResult:
        """
        Calculate maximum flow between two nodes.
        
        Uses edge weights as capacities. Useful for understanding
        the throughput capacity between regions of the network.
        
        Args:
            source: Source node hash
            target: Target node hash
            
        Returns:
            MaxFlowResult with flow value, edge flows, and path existence
        """
        # Handle missing nodes
        if not self.has_node(source) or not self.has_node(target):
            return MaxFlowResult(
                source=source,
                target=target,
                max_flow_value=0.0,
                flow_edges=[],
                path_exists=False,
            )
        
        try:
            # NetworkX returns (flow_value, flow_dict)
            # flow_dict[u][v] = flow on edge (u, v)
            flow_value, flow_dict = nx.maximum_flow(
                self.graph, source, target, capacity='weight'
            )
            
            # Build flow edges list with non-zero flows
            flow_edges = []
            for u, targets in flow_dict.items():
                for v, flow in targets.items():
                    if flow > 0:
                        # Get capacity (weight) from graph
                        edge_data = self.graph.get_edge_data(u, v, {})
                        capacity = edge_data.get('weight', 0)
                        flow_edges.append(MaxFlowEdge(
                            from_node=u,
                            to_node=v,
                            flow=float(flow),
                            capacity=float(capacity),
                        ))
            
            return MaxFlowResult(
                source=source,
                target=target,
                max_flow_value=float(flow_value),
                flow_edges=flow_edges,
                path_exists=flow_value > 0,
            )
            
        except nx.NetworkXError:
            return MaxFlowResult(
                source=source,
                target=target,
                max_flow_value=0.0,
                flow_edges=[],
                path_exists=False,
            )
        except nx.NetworkXUnbounded:
            # Infinite capacity (no weight constraints)
            return MaxFlowResult(
                source=source,
                target=target,
                max_flow_value=float('inf'),
                flow_edges=[],
                path_exists=True,
            )
    
    # === NODE METRICS ===
    
    def eccentricity(self, node_hash: str) -> Optional[int]:
        """
        Calculate eccentricity of a node.
        
        Eccentricity is the maximum shortest path length from this node
        to any other node. Lower eccentricity = more central.
        
        Args:
            node_hash: Node to calculate eccentricity for
            
        Returns:
            Eccentricity value or None if not calculable
        """
        if not self.has_node(node_hash):
            return None
        
        try:
            undirected = self.graph.to_undirected()
            return nx.eccentricity(undirected, node_hash)
        except nx.NetworkXError:
            return None
    
    def node_connectivity(self, source: str, target: str) -> int:
        """
        Calculate node connectivity between two nodes.
        
        This is the minimum number of nodes that must be removed
        to disconnect source from target.
        
        Args:
            source: Source node hash
            target: Target node hash
            
        Returns:
            Node connectivity value
        """
        if not self.has_node(source) or not self.has_node(target):
            return 0
        
        try:
            return nx.node_connectivity(self.graph, source, target)
        except nx.NetworkXError:
            return 0
    
    def local_efficiency(self) -> float:
        """
        Calculate local efficiency of the network.
        
        Local efficiency measures how well information is exchanged
        by the neighbors of each node when that node is removed.
        
        Returns:
            Local efficiency score (0-1)
        """
        if self.node_count < 2:
            return 0.0
        
        try:
            undirected = self.graph.to_undirected()
            return nx.local_efficiency(undirected)
        except nx.NetworkXError:
            return 0.0
    
    def global_efficiency(self) -> float:
        """
        Calculate global efficiency of the network.
        
        Global efficiency is the average inverse shortest path length.
        Higher efficiency = better overall connectivity.
        
        Returns:
            Global efficiency score (0-1)
        """
        if self.node_count < 2:
            return 0.0
        
        try:
            undirected = self.graph.to_undirected()
            return nx.global_efficiency(undirected)
        except nx.NetworkXError:
            return 0.0
    
    def to_dict(self) -> dict:
        """Export graph summary as dict."""
        return {
            "nodeCount": self.node_count,
            "edgeCount": self.edge_count,
            "builtAt": self.built_at,
            "isConnected": self.is_connected(),
            "density": round(self.density(), 4),
            "clusteringCoefficient": round(self.clustering_coefficient(), 4),
        }


def build_graph_from_db(
    conn_or_db: DBConnection,
    min_certain_count: int = DEFAULT_MIN_CERTAIN_COUNT,
    local_hash: Optional[str] = None,
) -> MeshGraph:
    """
    Build MeshGraph from edge_observations table.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        min_certain_count: Minimum certain observations to include edge
        local_hash: Local node's hash
        
    Returns:
        MeshGraph populated with validated edges
    """
    if not NETWORKX_AVAILABLE:
        raise ImportError("NetworkX is required. Install with: pip install networkx>=3.0")
    
    graph = MeshGraph(local_hash=local_hash)
    
    with ensure_connection(conn_or_db) as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                from_hash,
                to_hash,
                edge_key,
                observation_count,
                certain_count,
                confidence_sum,
                forward_count,
                reverse_count,
                last_seen
            FROM edge_observations
            WHERE certain_count >= ?
            ORDER BY certain_count DESC
        """, (min_certain_count,))
        
        for row in cursor.fetchall():
            from_hash = row[0]
            to_hash = row[1]
            edge_key = row[2]
            obs_count = row[3]
            certain_count = row[4]
            confidence_sum = row[5]
            forward_count = row[6]
            reverse_count = row[7]
            last_seen = row[8]
            
            # Calculate average confidence
            avg_confidence = confidence_sum / obs_count if obs_count > 0 else 0
            
            # Add edge with attributes
            graph.add_edge(
                from_hash,
                to_hash,
                edge_key=edge_key,
                weight=certain_count,  # Use certain count as weight
                observation_count=obs_count,
                certain_count=certain_count,
                avg_confidence=avg_confidence,
                forward_count=forward_count,
                reverse_count=reverse_count,
                last_seen=last_seen,
            )
    
    logger.debug(
        f"Built graph with {graph.node_count} nodes, "
        f"{graph.edge_count} edges from {min_certain_count}+ certain observations"
    )
    
    return graph


def get_shortest_path(
    conn_or_db: DBConnection,
    source: str,
    target: str,
    min_certain_count: int = DEFAULT_MIN_CERTAIN_COUNT,
) -> Optional[PathResult]:
    """
    Find shortest path between two nodes.
    
    Convenience function that builds graph and finds path.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        source: Source node hash (2-char prefix like "AB" or full hash)
        target: Target node hash
        min_certain_count: Minimum certainty for edges
        
    Returns:
        PathResult or None if no path exists
    """
    graph = build_graph_from_db(conn_or_db, min_certain_count)
    
    # Normalize to format used in graph (0xAB)
    if not source.startswith("0x"):
        source = f"0x{source.upper()}"
    if not target.startswith("0x"):
        target = f"0x{target.upper()}"
    
    return graph.shortest_path(source, target)


def get_all_paths(
    conn_or_db: DBConnection,
    source: str,
    target: str,
    cutoff: int = 6,
    min_certain_count: int = DEFAULT_MIN_CERTAIN_COUNT,
) -> List[PathResult]:
    """
    Find all simple paths between two nodes.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        source: Source node hash
        target: Target node hash
        cutoff: Maximum path length
        min_certain_count: Minimum certainty for edges
        
    Returns:
        List of PathResult objects
    """
    graph = build_graph_from_db(conn_or_db, min_certain_count)
    
    # Normalize
    if not source.startswith("0x"):
        source = f"0x{source.upper()}"
    if not target.startswith("0x"):
        target = f"0x{target.upper()}"
    
    return graph.all_paths(source, target, cutoff)


def get_node_centrality(
    conn_or_db: DBConnection,
    algorithm: str = "betweenness",
    min_certain_count: int = DEFAULT_MIN_CERTAIN_COUNT,
) -> CentralityResult:
    """
    Calculate node centrality using specified algorithm.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        algorithm: One of "betweenness", "degree", "pagerank", "closeness"
        min_certain_count: Minimum certainty for edges
        
    Returns:
        CentralityResult with scores
    """
    graph = build_graph_from_db(conn_or_db, min_certain_count)
    
    if algorithm == "betweenness":
        return graph.betweenness_centrality()
    elif algorithm == "degree":
        return graph.degree_centrality()
    elif algorithm == "pagerank":
        return graph.pagerank()
    elif algorithm == "closeness":
        return graph.closeness_centrality()
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")


def get_network_stats(
    conn_or_db: DBConnection,
    min_certain_count: int = DEFAULT_MIN_CERTAIN_COUNT,
) -> dict:
    """
    Get comprehensive network statistics.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        min_certain_count: Minimum certainty for edges
        
    Returns:
        Dict with network metrics
    """
    graph = build_graph_from_db(conn_or_db, min_certain_count)
    
    components = graph.connected_components()
    
    return {
        "nodeCount": graph.node_count,
        "edgeCount": graph.edge_count,
        "isConnected": graph.is_connected(),
        "componentCount": components.total_components,
        "largestComponentSize": components.largest_component_size,
        "isolatedNodeCount": len(components.isolated_nodes),
        "density": round(graph.density(), 4),
        "clusteringCoefficient": round(graph.clustering_coefficient(), 4),
        "diameter": graph.diameter(),
        "radius": graph.radius(),
        "averagePathLength": round(graph.average_path_length() or 0, 2),
        "builtAt": graph.built_at,
    }
