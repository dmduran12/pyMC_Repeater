"""
Analytics API - CherryPy endpoints for SQL-backed analytics
=============================================================

Provides REST endpoints for:
    - Bucketed airtime statistics
    - Topology graph data
    - Node activity sparklines
    - Path health metrics
    - Disambiguation statistics
    - Graph analysis (NetworkX-powered)
    - Network health and route simulation
    - Packet flow and geographic coverage

All endpoints return JSON responses with standardized error handling.

Error Response Format
--------------------
All errors follow a consistent format:

    {
        "success": False,
        "error": {
            "code": "INVALID_PARAMETER",
            "message": "...",
            "httpStatus": 400,
            "details": {...}
        }
    }

Success Response Format
-----------------------
    {
        "success": True,
        "data": {...}
    }
"""

import logging
import time
from typing import Optional, Dict, Any

import cherrypy

# Core database and utilities
from repeater.analytics.db import AnalyticsDB
from repeater.analytics.utils import normalize_hash, validate_hash

# Standardized error handling and validation
from repeater.analytics.errors import (
    ErrorCode,
    api_success,
    api_error,
    api_error_from_exception,
    invalid_param,
    missing_param,
    not_found,
)
from repeater.analytics.validation import (
    ValidationError,
    validate_hours,
    validate_limit,
    validate_min_certainty,
    validate_hash_param,
    validate_bool,
    validate_positive_int,
    validate_positive_float,
    validate_string_choice,
)

# Graph caching for performance
from repeater.analytics.cache import (
    get_cached_graph,
    invalidate_graph_cache,
    get_cache_stats,
)

# Feature modules
from repeater.analytics.bucketing import (
    get_airtime_utilization,
    get_airtime_utilization_preset,
    get_available_presets,
    TIME_RANGE_OPTIONS,
)
from repeater.analytics.topology import build_topology_from_db, load_latest_topology
from repeater.analytics.node_activity import get_all_sparklines
from repeater.analytics.path_registry import get_canonical_paths, calculate_path_health
from repeater.analytics.disambiguation import get_disambiguation_stats, build_prefix_lookup
from repeater.analytics.edge_builder import get_validated_edges
from repeater.analytics.last_hop import get_last_hop_neighbors
from repeater.analytics.neighbor_affinity import get_neighbor_affinity
from repeater.analytics.tx_delay import get_tx_recommendations
from repeater.analytics.mobile_detection import detect_mobile_nodes
from repeater.analytics.graph import (
    build_graph_from_db,
    get_shortest_path,
    get_all_paths,
    get_node_centrality,
    get_network_stats,
    NETWORKX_AVAILABLE,
)
from repeater.analytics.node_profile import get_node_profile
from repeater.analytics.network_health import get_network_health, get_quick_health_score
from repeater.analytics.route_simulation import simulate_route, compare_routes
from repeater.analytics.metric_history import analyze_trend, get_all_metric_trends, get_node_trends
from repeater.analytics.geographic_coverage import get_geographic_coverage, get_coverage_summary
from repeater.analytics.packet_flow import get_packet_flows, get_flow_matrix, get_top_talkers, get_flow_summary

logger = logging.getLogger("AnalyticsAPI")

# Centrality algorithm choices for validation
CENTRALITY_ALGORITHMS = ["betweenness", "degree", "pagerank", "closeness"]


class AnalyticsAPI:
    """
    CherryPy-mounted API for analytics endpoints.
    
    Mount at /api/analytics for URLs like:
        GET /api/analytics/bucketed_stats
        GET /api/analytics/topology
        GET /api/analytics/sparklines
        
    Error Handling:
        All endpoints use standardized error responses via the errors module.
        ValidationErrors from the validation module are automatically converted
        to proper API error responses.
        
    Caching:
        Graph-based endpoints use the cache module for performance. Graphs are
        cached by (min_certainty, local_hash) with configurable TTL.
    """
    
    def __init__(
        self,
        sqlite_path,
        local_hash: Optional[str] = None,
        neighbors_getter=None,
        radio_config_getter=None,
    ):
        """
        Initialize analytics API.
        
        Args:
            sqlite_path: Path to SQLite database
            local_hash: Local node's hash
            neighbors_getter: Callable returning neighbors dict
            radio_config_getter: Callable returning radio config dict
        """
        self.sqlite_path = sqlite_path
        self.local_hash = local_hash
        self.neighbors_getter = neighbors_getter
        self.radio_config_getter = radio_config_getter
        
        # Use AnalyticsDB for proper connection management with WAL mode
        self._db = AnalyticsDB(sqlite_path)
    
    def _get_connection(self):
        """
        Get database connection context manager.
        
        Returns an AnalyticsDB connection context that properly handles
        WAL mode, pragmas, and connection cleanup.
        """
        return self._db.connection()
    
    def _get_cached_graph(self, conn, min_certainty: int = 5):
        """
        Get graph from cache or build new one.
        
        Uses the module-level graph cache for performance.
        
        Args:
            conn: Database connection
            min_certainty: Minimum certainty for edges
            
        Returns:
            Cached MeshGraph instance
        """
        return get_cached_graph(
            conn,
            min_certainty=min_certainty,
            local_hash=self.local_hash,
        )
    
    def _require_networkx(self) -> Optional[Dict[str, Any]]:
        """
        Check if NetworkX is available.
        
        Returns:
            None if available, error response dict if not
        """
        if not NETWORKX_AVAILABLE:
            return api_error(
                ErrorCode.DEPENDENCY_MISSING,
                "NetworkX not installed. Run: pip install networkx>=3.0"
            )
        return None
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def bucketed_stats(self, preset=None, minutes=60, bucket_count=360):
        """
        GET /api/analytics/bucketed_stats
        
        Get bucketed packet statistics for airtime visualization.
        
        Query params:
            preset: Time range preset (e.g. "1h", "7d", "3mo") - preferred over minutes
            minutes: Time range in minutes (default: 60) - legacy support
            bucket_count: Number of buckets (default: 360) - only used with minutes
            
        Available presets:
            Short-term: 20m, 1h, 3h
            Medium-term: 12h, 1d, 2d, 3d, 4d, 5d, 6d, 7d
            Long-term: 14d, 30d, 3mo, 6mo
            Historical: 1yr, 2yr, 3yr, 4yr, 5yr, 10yr
            
        Returns:
            BucketedStats with received/transmitted/forwarded/dropped arrays
        """
        try:
            radio_config = None
            if self.radio_config_getter:
                radio_config = self.radio_config_getter()
            
            with self._get_connection() as conn:
                # Use preset if provided, otherwise fall back to minutes
                if preset:
                    stats = get_airtime_utilization_preset(
                        conn,
                        preset=preset,
                        radio_config=radio_config,
                    )
                else:
                    minutes_val = validate_positive_int(minutes, "minutes", default=60, min_value=1)
                    bucket_val = validate_positive_int(bucket_count, "bucket_count", default=360, min_value=1)
                    stats = get_airtime_utilization(
                        conn,
                        minutes=minutes_val,
                        bucket_count=bucket_val,
                        radio_config=radio_config,
                    )
                
                return api_success(stats.to_dict())
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error getting bucketed stats: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def time_presets(self):
        """
        GET /api/analytics/time_presets
        
        Get available time range presets for UI dropdowns.
        
        Returns:
            List of preset objects with key, label, total_seconds, bucket_seconds
        """
        try:
            presets = get_available_presets()
            return api_success({
                "presets": presets,
                "default": "1h",
            })
        except Exception as e:
            logger.error(f"Error getting time presets: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def topology(self, rebuild=False):
        """
        GET /api/analytics/topology
        
        Get precomputed topology data.
        
        Query params:
            rebuild: If "true", force rebuild instead of loading cached
            
        Returns:
            TopologySnapshot with edges, hub nodes, centrality, loops
        """
        try:
            rebuild_val = validate_bool(rebuild, "rebuild", default=False)
            
            with self._get_connection() as conn:
                if rebuild_val:
                    snapshot = build_topology_from_db(conn, self.local_hash)
                    # Invalidate graph cache when topology is rebuilt
                    invalidate_graph_cache()
                else:
                    snapshot = load_latest_topology(conn)
                    if not snapshot:
                        snapshot = build_topology_from_db(conn, self.local_hash)
                
                return api_success(snapshot.to_dict())
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error getting topology: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def topology_edges(self, min_certainty=15, limit=500):
        """
        GET /api/analytics/topology_edges
        
        Get validated topology edges only (lighter than full topology).
        
        Query params:
            min_certainty: Minimum certain count (default: 15)
            limit: Maximum edges to return (default: 500)
            
        Returns:
            List of edge dicts
        """
        try:
            min_cert = validate_min_certainty(min_certainty, default=15)
            lim = validate_limit(limit, default=500)
            
            with self._get_connection() as conn:
                edges = get_validated_edges(
                    conn,
                    min_certain_count=min_cert,
                    limit=lim,
                )
                
                return api_success(edges, count=len(edges))
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error getting topology edges: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def sparklines(self, hours=168):
        """
        GET /api/analytics/sparklines
        
        Get activity sparklines for all nodes.
        
        Query params:
            hours: Time range in hours (default: 168 = 7 days)
            
        Returns:
            Dict mapping node hash -> sparkline data array
        """
        try:
            hours_val = validate_hours(hours)
            
            with self._get_connection() as conn:
                sparklines_data = get_all_sparklines(conn, hours=hours_val)
                
                return api_success({
                    "hours": hours_val,
                    "bucket_hours": 6,
                    "sparklines": sparklines_data,
                })
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error getting sparklines: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def paths(self, limit=100, min_observations=5):
        """
        GET /api/analytics/paths
        
        Get canonical (most-used) paths.
        
        Query params:
            limit: Maximum paths to return (default: 100)
            min_observations: Minimum observations to include (default: 5)
            
        Returns:
            List of ObservedPath dicts
        """
        try:
            lim = validate_limit(limit)
            min_obs = validate_positive_int(min_observations, "min_observations", default=5, min_value=1)
            
            with self._get_connection() as conn:
                paths = get_canonical_paths(
                    conn,
                    limit=lim,
                    min_observations=min_obs,
                )
                
                return api_success(
                    [p.to_dict() for p in paths],
                    count=len(paths),
                )
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error getting paths: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def path_health(self, limit=20):
        """
        GET /api/analytics/path_health
        
        Get path health indicators.
        
        Query params:
            limit: Maximum paths to analyze (default: 20)
            
        Returns:
            List of PathHealth dicts
        """
        try:
            lim = validate_limit(limit, default=20)
            
            with self._get_connection() as conn:
                # Get edges and hub nodes for health calculation
                edges = get_validated_edges(conn, min_certain_count=5, limit=500)
                
                # Get hub nodes from latest topology
                snapshot = load_latest_topology(conn)
                hub_nodes = snapshot.hub_nodes if snapshot else []
                
                health = calculate_path_health(
                    conn,
                    edges=edges,
                    hub_nodes=hub_nodes,
                    limit=lim,
                )
                
                return api_success(
                    [h.to_dict() for h in health],
                    count=len(health),
                )
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error getting path health: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def disambiguation_stats(self):
        """
        GET /api/analytics/disambiguation_stats
        
        Get prefix disambiguation statistics.
        
        Returns:
            DisambiguationStats dict
        """
        try:
            neighbors = {}
            if self.neighbors_getter:
                neighbors = self.neighbors_getter()
            
            with self._get_connection() as conn:
                stats = get_disambiguation_stats(
                    conn,
                    neighbors=neighbors,
                    local_hash=self.local_hash,
                )
                
                return api_success(stats.to_dict())
                
        except Exception as e:
            logger.error(f"Error getting disambiguation stats: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def last_hop_neighbors(self, hours=168):
        """
        GET /api/analytics/last_hop_neighbors
        
        Get direct RF neighbors detected from last-hop analysis.
        
        Query params:
            hours: Time range in hours (default: 168 = 7 days)
            
        Returns:
            LastHopStats with neighbor list and signal data
        """
        try:
            hours_val = validate_hours(hours)
            
            neighbors = {}
            if self.neighbors_getter:
                neighbors = self.neighbors_getter()
            
            with self._get_connection() as conn:
                stats = get_last_hop_neighbors(
                    conn,
                    neighbors=neighbors,
                    local_hash=self.local_hash,
                    hours=hours_val,
                )
                
                return api_success(stats.to_dict())
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error getting last hop neighbors: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def neighbor_affinity(self, hours=168, min_observations=5):
        """
        GET /api/analytics/neighbor_affinity
        
        Get neighbor affinity scores based on path co-occurrence.
        
        Query params:
            hours: Time range in hours (default: 168 = 7 days)
            min_observations: Minimum observations to include (default: 5)
            
        Returns:
            AffinityStats with neighbor pair affinities
        """
        try:
            hours = int(hours)
            min_observations = int(min_observations)
            
            neighbors = {}
            if self.neighbors_getter:
                neighbors = self.neighbors_getter()
            
            with self._get_connection() as conn:
                stats = get_neighbor_affinity(
                    conn,
                    neighbors=neighbors,
                    local_hash=self.local_hash,
                    hours=hours,
                    min_observations=min_observations,
                )
                
                return api_success(stats.to_dict())
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error getting neighbor affinity: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def tx_recommendations(self, hours=24):
        """
        GET /api/analytics/tx_recommendations
        
        Get TX delay factor recommendations based on network role.
        
        Query params:
            hours: Time range for traffic analysis (default: 24)
            
        Returns:
            TxDelayRecommendation with optimized factors
        """
        try:
            hours = int(hours)
            
            with self._get_connection() as conn:
                recommendation = get_tx_recommendations(
                    conn,
                    local_hash=self.local_hash,
                    hours=hours,
                )
                
                return api_success(recommendation.to_dict())
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error getting TX recommendations: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def mobile_nodes(self, hours=168, min_observations=10):
        """
        GET /api/analytics/mobile_nodes
        
        Detect mobile nodes based on path volatility.
        
        Query params:
            hours: Time range in hours (default: 168 = 7 days)
            min_observations: Minimum observations for classification (default: 10)
            
        Returns:
            MobileDetectionStats with node classifications
        """
        try:
            hours = int(hours)
            min_observations = int(min_observations)
            
            neighbors = {}
            if self.neighbors_getter:
                neighbors = self.neighbors_getter()
            
            with self._get_connection() as conn:
                stats = detect_mobile_nodes(
                    conn,
                    neighbors=neighbors,
                    local_hash=self.local_hash,
                    hours=hours,
                    min_observations=min_observations,
                )
                
                return api_success(stats.to_dict())
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error detecting mobile nodes: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def disambiguation(self, prefix=None, hours=168):
        """
        GET /api/analytics/disambiguation
        
        Get full disambiguation lookup or resolve a specific prefix.
        
        Query params:
            prefix: Optional prefix to resolve (e.g. "AB")
            hours: Time range in hours (default: 168 = 7 days)
            
        Returns:
            If prefix provided: Resolution result for that prefix
            If no prefix: Full disambiguation statistics
        """
        try:
            hours = int(hours)
            
            neighbors = {}
            if self.neighbors_getter:
                neighbors = self.neighbors_getter()
            
            with self._get_connection() as conn:
                lookup = build_prefix_lookup(
                    conn,
                    neighbors=neighbors,
                    local_hash=self.local_hash,
                    hours=hours,
                )
                
                if prefix:
                    result = lookup.get(prefix)
                    if result:
                        return api_success(result.to_dict())
                    else:
                        return api_success({
                            "prefix": prefix.upper(),
                            "candidates": [],
                            "bestMatch": None,
                            "confidence": 0,
                            "isUnambiguous": False,
                        })
                else:
                    return api_success(lookup.get_stats().to_dict())
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error getting disambiguation: {e}")
            return api_error_from_exception(e)
    
    # === GRAPH ANALYSIS ENDPOINTS ===
    # Note: These endpoints use cached graphs for performance.
    # Cache TTL and settings are configurable via Config.CACHE.
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def graph_stats(self, min_certainty=5):
        """
        GET /api/analytics/graph_stats
        
        Get comprehensive network graph statistics.
        
        Query params:
            min_certainty: Minimum certain count for edges (default: 5)
            
        Returns:
            Network metrics including node count, edge count, diameter,
            clustering coefficient, connected components, etc.
        """
        nx_error = self._require_networkx()
        if nx_error:
            return nx_error
        
        try:
            min_cert = validate_min_certainty(min_certainty)
            
            with self._get_connection() as conn:
                stats = get_network_stats(conn, min_certain_count=min_cert)
                return api_success(stats)
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error getting graph stats: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def graph_shortest_path(self, source=None, target=None, min_certainty=5):
        """
        GET /api/analytics/graph_shortest_path
        
        Find shortest path between two nodes.
        
        Query params:
            source: Source node hash (e.g., "AB" or "0xAB")
            target: Target node hash
            min_certainty: Minimum certain count for edges (default: 5)
            
        Returns:
            PathResult with path, hop count, and edge keys
        """
        nx_error = self._require_networkx()
        if nx_error:
            return nx_error
        
        try:
            source_hash = validate_hash_param(source, "source")
            target_hash = validate_hash_param(target, "target")
            min_cert = validate_min_certainty(min_certainty)
            
            with self._get_connection() as conn:
                result = get_shortest_path(
                    conn,
                    source=source_hash,
                    target=target_hash,
                    min_certain_count=min_cert,
                )
                
                if result:
                    return api_success(result.to_dict())
                else:
                    return api_success({
                        "source": source_hash,
                        "target": target_hash,
                        "path": None,
                        "message": "No path found between nodes",
                    })
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error finding shortest path: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def graph_all_paths(self, source=None, target=None, cutoff=6, min_certainty=5):
        """
        GET /api/analytics/graph_all_paths
        
        Find all simple paths between two nodes.
        
        Query params:
            source: Source node hash
            target: Target node hash
            cutoff: Maximum path length (default: 6)
            min_certainty: Minimum certain count for edges (default: 5)
            
        Returns:
            List of PathResult objects sorted by hop count
        """
        nx_error = self._require_networkx()
        if nx_error:
            return nx_error
        
        try:
            source_hash = validate_hash_param(source, "source")
            target_hash = validate_hash_param(target, "target")
            cutoff_val = validate_positive_int(cutoff, "cutoff", default=6, min_value=1, max_value=10)
            min_cert = validate_min_certainty(min_certainty)
            
            with self._get_connection() as conn:
                results = get_all_paths(
                    conn,
                    source=source_hash,
                    target=target_hash,
                    cutoff=cutoff_val,
                    min_certain_count=min_cert,
                )
                
                return api_success(
                    [r.to_dict() for r in results],
                    count=len(results),
                )
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error finding all paths: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def graph_centrality(self, algorithm="betweenness", min_certainty=5):
        """
        GET /api/analytics/graph_centrality
        
        Calculate node centrality using various algorithms.
        
        Query params:
            algorithm: One of "betweenness", "degree", "pagerank", "closeness"
            min_certainty: Minimum certain count for edges (default: 5)
            
        Returns:
            CentralityResult with scores for all nodes and top 10 list
        """
        nx_error = self._require_networkx()
        if nx_error:
            return nx_error
        
        try:
            algo = validate_string_choice(algorithm, "algorithm", CENTRALITY_ALGORITHMS, default="betweenness")
            min_cert = validate_min_certainty(min_certainty)
            
            with self._get_connection() as conn:
                result = get_node_centrality(
                    conn,
                    algorithm=algo,
                    min_certain_count=min_cert,
                )
                
                return api_success(result.to_dict())
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error calculating centrality: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def graph_components(self, min_certainty=5):
        """
        GET /api/analytics/graph_components
        
        Find connected components in the network graph.
        
        Query params:
            min_certainty: Minimum certain count for edges (default: 5)
            
        Returns:
            ComponentResult with component lists and isolated nodes
        """
        nx_error = self._require_networkx()
        if nx_error:
            return nx_error
        
        try:
            min_cert = validate_min_certainty(min_certainty)
            
            with self._get_connection() as conn:
                graph = self._get_cached_graph(conn, min_cert)
                result = graph.connected_components()
                
                return api_success(result.to_dict())
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error finding components: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def graph_neighbors(self, node=None, min_certainty=5):
        """
        GET /api/analytics/graph_neighbors
        
        Get all neighbors of a specific node in the graph.
        
        Query params:
            node: Node hash (e.g., "AB" or "0xAB")
            min_certainty: Minimum certain count for edges (default: 5)
            
        Returns:
            List of neighbor node hashes and their edge attributes
        """
        nx_error = self._require_networkx()
        if nx_error:
            return nx_error
        
        try:
            node_hash = validate_hash_param(node, "node")
            min_cert = validate_min_certainty(min_certainty)
            
            with self._get_connection() as conn:
                graph = self._get_cached_graph(conn, min_cert)
                
                if not graph.has_node(node_hash):
                    return api_success({
                        "node": node_hash,
                        "neighbors": [],
                        "degree": 0,
                        "message": "Node not found in graph",
                    })
                
                neighbors = graph.neighbors(node_hash)
                
                # Get edge attributes for each neighbor
                neighbor_details = []
                for neighbor in neighbors:
                    edge_data = graph.graph.get_edge_data(node_hash, neighbor, {})
                    neighbor_details.append({
                        "node": neighbor,
                        "edgeKey": graph.get_edge_key(node_hash, neighbor),
                        "weight": edge_data.get("weight", 0),
                        "certainCount": edge_data.get("certain_count", 0),
                        "avgConfidence": round(edge_data.get("avg_confidence", 0), 3),
                    })
                
                return api_success({
                    "node": node_hash,
                    "neighbors": neighbor_details,
                    "degree": len(neighbors),
                })
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error getting neighbors: {e}")
            return api_error_from_exception(e)

    # === NEW ANALYTICS ENDPOINTS ===
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def node_profile(self, node=None, hours=168, min_certainty=5):
        """
        GET /api/analytics/node_profile
        
        Get comprehensive profile for a single node.
        
        Query params:
            node: Node hash or prefix (e.g., "0xAB123456" or "AB")
            hours: Time range for activity analysis (default: 168 = 7 days)
            min_certainty: Minimum certainty for graph edges (default: 5)
            
        Returns:
            NodeProfile with identity, role, connectivity, traffic, activity,
            signal quality, and location data.
        """
        if not node:
            return missing_param("node")
        
        try:
            hours = int(hours)
            min_certainty = int(min_certainty)
            
            neighbors = {}
            if self.neighbors_getter:
                neighbors = self.neighbors_getter()
            
            # Get hub/gateway nodes from topology
            hub_nodes = []
            gateway_nodes = []
            with self._get_connection() as conn:
                from repeater.analytics.topology import load_latest_topology
                topology = load_latest_topology(conn)
                if topology:
                    hub_nodes = topology.hub_nodes
                    gateway_nodes = topology.gateway_nodes
            
            with self._get_connection() as conn:
                profile = get_node_profile(
                    conn,
                    node=node,
                    neighbors=neighbors,
                    hub_nodes=hub_nodes,
                    gateway_nodes=gateway_nodes,
                    local_hash=self.local_hash,
                    hours=hours,
                    min_certainty=min_certainty,
                )
                
                if profile:
                    return api_success(profile.to_dict())
                else:
                    return api_error_from_exception(f"Node '{node}' not found")
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error getting node profile: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def network_health(self, hours=168, min_certainty=5):
        """
        GET /api/analytics/network_health
        
        Get comprehensive network health report.
        
        Query params:
            hours: Time range for stability analysis (default: 168 = 7 days)
            min_certainty: Minimum certainty for graph edges (default: 5)
            
        Returns:
            NetworkHealthReport with:
            - Overall health score (0-1)
            - Component scores (connectivity, redundancy, stability, coverage)
            - Detected issues with severity
            - Actionable recommendations
        """
        try:
            hours = int(hours)
            min_certainty = int(min_certainty)
            
            with self._get_connection() as conn:
                report = get_network_health(
                    conn,
                    local_hash=self.local_hash,
                    min_certainty=min_certainty,
                    hours=hours,
                )
                
                return api_success(report.to_dict())
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error getting network health: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def quick_health(self, min_certainty=5):
        """
        GET /api/analytics/quick_health
        
        Get just the overall health score (faster than full report).
        
        Query params:
            min_certainty: Minimum certainty for graph edges (default: 5)
            
        Returns:
            Quick health score (0-1)
        """
        try:
            min_certainty = int(min_certainty)
            
            with self._get_connection() as conn:
                score = get_quick_health_score(
                    conn,
                    local_hash=self.local_hash,
                    min_certainty=min_certainty,
                )
                
                return api_success({"healthScore": round(score, 3)})
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error getting quick health: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def simulate_route(self, source=None, target=None, via=None, min_certainty=5, hours=168):
        """
        GET /api/analytics/simulate_route
        
        Simulate and predict routing between two nodes.
        
        Query params:
            source: Source node hash or prefix (required)
            target: Target node hash or prefix (required)
            via: Optional waypoint to route through
            min_certainty: Minimum certainty for graph edges (default: 5)
            hours: Time range for historical data (default: 168)
            
        Returns:
            RouteSimulation with:
            - Predicted path and hop count
            - Estimated latency (with range)
            - Success probability
            - Alternative paths
            - Bottleneck analysis
        """
        if not source:
            return missing_param("source")
        if not target:
            return missing_param("target")
        
        try:
            min_certainty = int(min_certainty)
            hours = int(hours)
            
            with self._get_connection() as conn:
                result = simulate_route(
                    conn,
                    source=source,
                    target=target,
                    via=via,
                    local_hash=self.local_hash,
                    min_certainty=min_certainty,
                    hours=hours,
                )
                
                return api_success(result.to_dict())
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error simulating route: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def trends(self, metric=None, node=None, days=7):
        """
        GET /api/analytics/trends
        
        Get trend analysis for metrics over time.
        
        Query params:
            metric: Metric type to analyze (e.g., "health_score", "centrality")
                   If not provided, returns all network-wide metric trends.
            node: Node hash for per-node metrics (optional)
            days: Number of days to analyze (default: 7)
            
        Returns:
            TrendAnalysis with:
            - Direction (increasing, decreasing, stable, volatile)
            - Statistical measures (slope, r-squared)
            - Summary stats (min, max, avg, std_dev)
            - Change metrics (absolute and percent)
            - Historical data points
        """
        try:
            days = int(days)
            
            with self._get_connection() as conn:
                if metric:
                    # Single metric trend
                    result = analyze_trend(
                        conn,
                        metric_type=metric,
                        node_hash=node,
                        days=days,
                    )
                    return api_success(result.to_dict())
                elif node:
                    # All metrics for a specific node
                    results = get_node_trends(conn, node_hash=node, days=days)
                    return api_success({
                        "nodeHash": node,
                        "trends": {k: v.to_dict() for k, v in results.items()},
                    })
                else:
                    # All network-wide metrics
                    results = get_all_metric_trends(conn, days=days)
                    return api_success({
                        "trends": {k: v.to_dict() for k, v in results.items()},
                    })
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error getting trends: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def graph_bridges(self, min_certainty=5):
        """
        GET /api/analytics/graph_bridges
        
        Find bridge edges (single points of failure).
        
        Query params:
            min_certainty: Minimum certain count for edges (default: 5)
            
        Returns:
            List of bridge edges that are critical to network connectivity.
        """
        nx_error = self._require_networkx()
        if nx_error:
            return nx_error
        
        try:
            min_cert = validate_min_certainty(min_certainty)
            
            with self._get_connection() as conn:
                graph = self._get_cached_graph(conn, min_cert)
                bridges = graph.find_bridges()
                
                # Convert to edge info
                bridge_info = []
                for u, v in bridges:
                    edge_key = graph.get_edge_key(u, v)
                    bridge_info.append({
                        "from": u,
                        "to": v,
                        "edgeKey": edge_key,
                    })
                
                return api_success({
                    "bridges": bridge_info,
                    "count": len(bridges),
                })
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error finding bridges: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def graph_articulation_points(self, min_certainty=5):
        """
        GET /api/analytics/graph_articulation_points
        
        Find articulation points (critical nodes).
        
        Query params:
            min_certainty: Minimum certain count for edges (default: 5)
            
        Returns:
            List of nodes that are critical to network connectivity.
        """
        nx_error = self._require_networkx()
        if nx_error:
            return nx_error
        
        try:
            min_cert = validate_min_certainty(min_certainty)
            
            with self._get_connection() as conn:
                graph = self._get_cached_graph(conn, min_cert)
                points = graph.find_articulation_points()
                
                return api_success({
                    "articulationPoints": points,
                    "count": len(points),
                })
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error finding articulation points: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def graph_communities(self, min_certainty=5):
        """
        GET /api/analytics/graph_communities
        
        Detect network communities using modularity optimization.
        
        Query params:
            min_certainty: Minimum certain count for edges (default: 5)
            
        Returns:
            List of communities (each is a list of node hashes).
        """
        nx_error = self._require_networkx()
        if nx_error:
            return nx_error
        
        try:
            min_cert = validate_min_certainty(min_certainty)
            
            with self._get_connection() as conn:
                graph = self._get_cached_graph(conn, min_cert)
                communities = graph.detect_communities()
                
                return api_success({
                    "communities": communities,
                    "count": len(communities),
                    "sizes": [len(c) for c in communities],
                })
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error detecting communities: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def graph_resilience(self, min_certainty=5):
        """
        GET /api/analytics/graph_resilience
        
        Calculate network resilience score.
        
        Query params:
            min_certainty: Minimum certain count for edges (default: 5)
            
        Returns:
            Resilience score (0-1) and component breakdown.
        """
        nx_error = self._require_networkx()
        if nx_error:
            return nx_error
        
        try:
            min_cert = validate_min_certainty(min_certainty)
            
            with self._get_connection() as conn:
                graph = self._get_cached_graph(conn, min_cert)
                resilience = graph.resilience_score()
                
                # Get component metrics for breakdown
                bridges = graph.find_bridges()
                articulation_pts = graph.find_articulation_points()
                components = graph.connected_components()
                
                return api_success({
                    "resilience": round(resilience, 3),
                    "bridgeCount": len(bridges),
                    "articulationPointCount": len(articulation_pts),
                    "componentCount": components.total_components,
                    "isConnected": graph.is_connected(),
                    "nodeCount": graph.node_count,
                    "edgeCount": graph.edge_count // 2,
                })
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error calculating resilience: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def graph_k_paths(self, source=None, target=None, k=5, min_certainty=5):
        """
        GET /api/analytics/graph_k_paths
        
        Find K shortest paths between two nodes.
        
        Query params:
            source: Source node hash (required)
            target: Target node hash (required)
            k: Number of paths to find (default: 5)
            min_certainty: Minimum certain count for edges (default: 5)
            
        Returns:
            List of K shortest paths sorted by hop count.
        """
        nx_error = self._require_networkx()
        if nx_error:
            return nx_error
        
        try:
            source_hash = validate_hash_param(source, "source")
            target_hash = validate_hash_param(target, "target")
            k_val = validate_positive_int(k, "k", default=5, min_value=1, max_value=20)
            min_cert = validate_min_certainty(min_certainty)
            
            with self._get_connection() as conn:
                graph = self._get_cached_graph(conn, min_cert)
                paths = graph.k_shortest_paths(source_hash, target_hash, k=k_val)
                
                return api_success({
                    "paths": [p.to_dict() for p in paths],
                    "count": len(paths),
                })
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error finding K shortest paths: {e}")
            return api_error_from_exception(e)

    # === GEOGRAPHIC COVERAGE ENDPOINTS ===
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def geo_coverage(self, hours=168, grid_resolution=5, num_suggestions=5):
        """
        GET /api/analytics/geo_coverage
        
        Get comprehensive geographic coverage analysis.
        
        Query params:
            hours: Time range for node activity (default: 168 = 7 days)
            grid_resolution: Grid cell size in km for analysis (default: 5)
            num_suggestions: Number of placement suggestions (default: 5)
            
        Returns:
            GeographicCoverage with:
            - Nodes with coordinates
            - Convex hull (coverage boundary)
            - Coverage gaps
            - Density regions
            - Placement suggestions
            - Coverage score
        """
        try:
            hours = int(hours)
            grid_resolution = float(grid_resolution)
            num_suggestions = int(num_suggestions)
            
            with self._get_connection() as conn:
                coverage = get_geographic_coverage(
                    conn,
                    hours=hours,
                    grid_resolution_km=grid_resolution,
                    num_placement_suggestions=num_suggestions,
                )
                
                return api_success(coverage.to_dict())
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error getting geographic coverage: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def geo_coverage_summary(self, hours=168):
        """
        GET /api/analytics/geo_coverage_summary
        
        Get quick geographic coverage summary (faster than full analysis).
        
        Query params:
            hours: Time range for node activity (default: 168 = 7 days)
            
        Returns:
            Quick summary with node counts, area, and distances.
        """
        try:
            hours = int(hours)
            
            with self._get_connection() as conn:
                summary = get_coverage_summary(conn, hours=hours)
                return api_success(summary)
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error getting coverage summary: {e}")
            return api_error_from_exception(e)
    
    # === PACKET FLOW ENDPOINTS ===
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def packet_flow(self, hours=24, min_packets=5, max_links=100):
        """
        GET /api/analytics/packet_flow
        
        Get aggregated packet flows for Sankey diagram visualization.
        
        Query params:
            hours: Time range in hours (default: 24)
            min_packets: Minimum packets for a flow to be included (default: 5)
            max_links: Maximum number of links to return (default: 100)
            
        Returns:
            SankeyData with:
            - nodes: List of FlowNode objects
            - links: List of FlowLink objects with source/target indices
            - totalPackets, totalBytes, timeRangeHours
        """
        try:
            hours = int(hours)
            min_packets = int(min_packets)
            max_links = int(max_links)
            
            with self._get_connection() as conn:
                flows = get_packet_flows(
                    conn,
                    hours=hours,
                    min_packets=min_packets,
                    local_hash=self.local_hash,
                    max_links=max_links,
                )
                
                return api_success(flows.to_dict())
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error getting packet flows: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def flow_matrix(self, hours=24, min_packets=1, limit_nodes=20):
        """
        GET /api/analytics/flow_matrix
        
        Get source-destination flow matrix.
        
        Query params:
            hours: Time range in hours (default: 24)
            min_packets: Minimum packets for inclusion (default: 1)
            limit_nodes: Maximum nodes to include (default: 20)
            
        Returns:
            FlowMatrix with nodes list, packet count matrix, and node names.
        """
        try:
            hours = int(hours)
            min_packets = int(min_packets)
            limit_nodes = int(limit_nodes)
            
            with self._get_connection() as conn:
                matrix = get_flow_matrix(
                    conn,
                    hours=hours,
                    min_packets=min_packets,
                    limit_nodes=limit_nodes,
                )
                
                return api_success(matrix.to_dict())
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error getting flow matrix: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def top_talkers(self, hours=24, limit=10):
        """
        GET /api/analytics/top_talkers
        
        Get nodes with highest traffic volume.
        
        Query params:
            hours: Time range in hours (default: 24)
            limit: Number of top talkers to return (default: 10)
            
        Returns:
            List of TopTalker objects sorted by total traffic.
        """
        try:
            hours = int(hours)
            limit = int(limit)
            
            with self._get_connection() as conn:
                talkers = get_top_talkers(conn, hours=hours, limit=limit)
                return api_success({
                    "topTalkers": [t.to_dict() for t in talkers],
                    "count": len(talkers),
                    "timeRangeHours": hours,
                })
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error getting top talkers: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def flow_summary(self, hours=24):
        """
        GET /api/analytics/flow_summary
        
        Get quick summary of packet flows.
        
        Query params:
            hours: Time range in hours (default: 24)
            
        Returns:
            Quick summary with total packets, bytes, unique sources/destinations.
        """
        try:
            hours = int(hours)
            
            with self._get_connection() as conn:
                summary = get_flow_summary(conn, hours=hours)
                return api_success(summary)
                
        except ValueError as e:
            return invalid_param("parameter", str(e))
        except Exception as e:
            logger.error(f"Error getting flow summary: {e}")
            return api_error_from_exception(e)
    
    # === NETWORK FLOW (MAX FLOW) ENDPOINT ===
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def graph_max_flow(self, source=None, target=None, min_certainty=5):
        """
        GET /api/analytics/graph_max_flow
        
        Calculate maximum flow between two nodes.
        
        Uses edge packet counts as capacities to find the theoretical
        maximum throughput between source and target.
        
        Query params:
            source: Source node hash (required)
            target: Target node hash (required)
            min_certainty: Minimum certain count for edges (default: 5)
            
        Returns:
            Maximum flow value and the flow on each edge.
        """
        nx_error = self._require_networkx()
        if nx_error:
            return nx_error
        
        try:
            source_hash = validate_hash_param(source, "source")
            target_hash = validate_hash_param(target, "target")
            min_cert = validate_min_certainty(min_certainty)
            
            with self._get_connection() as conn:
                graph = self._get_cached_graph(conn, min_cert)
                result = graph.max_flow(source_hash, target_hash)
                
                return api_success(result.to_dict())
                
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error calculating max flow: {e}")
            return api_error_from_exception(e)
    
    # === CACHE MANAGEMENT ENDPOINTS ===
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def cache_info(self):
        """
        GET /api/analytics/cache_info
        
        Get graph cache statistics and information.
        
        Returns:
            CacheInfo with:
            - ttlSeconds: Cache time-to-live
            - maxEntries: Maximum cached graphs
            - currentSize: Number of cached entries
            - buildingCount: Entries currently being built
            - entries: List of cached entry details
            - stats: Hit/miss statistics
        """
        try:
            cache_data = get_cache_stats()
            return api_success(cache_data)
        except Exception as e:
            logger.error(f"Error getting cache info: {e}")
            return api_error_from_exception(e)
    
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def cache_invalidate(self, min_certainty=None):
        """
        POST /api/analytics/cache_invalidate
        
        Invalidate graph cache entries.
        
        Query params:
            min_certainty: Specific certainty to invalidate (optional)
                          If not provided, invalidates all entries.
            
        Returns:
            Number of entries invalidated.
        """
        try:
            min_cert = None
            if min_certainty is not None and min_certainty != "":
                min_cert = validate_min_certainty(min_certainty)
            
            count = invalidate_graph_cache(min_certainty=min_cert)
            return api_success({
                "invalidated": count,
                "minCertainty": min_cert,
            })
            
        except ValidationError as e:
            return e.to_response()
        except Exception as e:
            logger.error(f"Error invalidating cache: {e}")
            return api_error_from_exception(e)

    @cherrypy.expose
    def default(self, *args, **kwargs):
        """Handle unmatched routes."""
        if cherrypy.request.method == "OPTIONS":
            return ""
        raise cherrypy.HTTPError(404, "Analytics endpoint not found")
