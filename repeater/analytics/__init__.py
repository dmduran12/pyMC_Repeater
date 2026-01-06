"""
Analytics Module for pyMC_Repeater
================================

Provides SQL-backed analytics for mesh topology, time-series metrics,
and node activity tracking. Moves heavy computation from frontend
Web Workers to backend for instant page loads and persistent state.

Architecture Overview
---------------------
This module replaces client-side analytics (pymc_console's Web Workers)
with server-side SQL-backed computation. Benefits:

    1. **Instant page loads** - No 200K packet fetch on dashboard open
    2. **Persistent state** - Analytics survive browser refresh
    3. **Shared computation** - All clients see same precomputed data
    4. **Historical depth** - Query years of data without memory limits

Components
----------
    edge_builder:
        Extracts topology edges from packet forwarding paths.
        Tracks confidence scores and bidirectional observations.
        
    bucketing:
        Time-series aggregation with 21 preset time ranges (20m to 10yr).
        Supports airtime utilization charts and packet statistics.
        
    node_activity:
        Sparkline computation using 6-hour buckets over 7 days.
        Tracks per-node packet activity for dashboard visualization.
        
    topology:
        Graph analysis including centrality scores, hub detection,
        gateway identification, and network loop detection.
        
    disambiguation:
        Resolves 2-character hash prefix collisions using multiple
        signals: position consistency, co-occurrence, recency.
        
    path_registry:
        Tracks observed packet paths and computes health metrics.
        Identifies canonical (most-used) routes through the mesh.
        
    worker:
        Background thread that processes packets incrementally and
        periodically rebuilds aggregate statistics.

Database Tables
---------------
Created automatically by AnalyticsWorker._init_tables():

    - edge_observations: Topology edge tracking
    - node_activity: Per-node activity buckets
    - path_observations: Observed packet paths
    - topology_snapshot: Precomputed topology cache

API Endpoints
-------------
Mounted at /api/analytics/ by http_server.py:

    GET /api/analytics/bucketed_stats?preset=7d
    GET /api/analytics/time_presets
    GET /api/analytics/topology
    GET /api/analytics/topology_edges
    GET /api/analytics/sparklines
    GET /api/analytics/paths
    GET /api/analytics/path_health
    GET /api/analytics/disambiguation_stats

Usage Example
-------------
    # Start analytics with the daemon (automatic via storage_collector)
    storage_collector.start_analytics_worker()
    
    # Query via API
    GET /api/analytics/bucketed_stats?preset=7d
    
    # Or programmatically
    from repeater.analytics import get_airtime_utilization_preset
    with sqlite3.connect(db_path) as conn:
        stats = get_airtime_utilization_preset(conn, preset="7d")

See Also
--------
    - repeater/web/analytics_api.py: REST endpoint implementation
    - repeater/data_acquisition/storage_collector.py: Worker integration
    - pymc_console/frontend/src/lib/: Original client-side implementation
"""

from .edge_builder import (
    extract_edges_from_packet,
    EdgeObservation,
)
from .bucketing import (
    compute_time_bucket,
    BucketedStats,
    UtilBucket,
    TIME_RANGE_PRESETS,
    TIME_RANGE_OPTIONS,
    parse_time_range,
    get_available_presets,
    get_airtime_utilization_preset,
)
from .node_activity import (
    update_node_activity,
    ActivityBucket,
)
from .topology import (
    TopologySnapshot,
    TopologyEdge,
)
from .disambiguation import (
    DisambiguationCandidate,
    DisambiguationResult,
    DisambiguationStats,
    PrefixLookup,
)
from .path_registry import (
    ObservedPath,
    PathHealth,
)
from .worker import AnalyticsWorker
from .db import (
    AnalyticsDB,
    AnalyticsDBError,
    ConnectionError,
    QueryError,
    DBConnection,
    ensure_connection,
    with_connection,
)
from .graph import (
    MeshGraph,
    PathResult,
    CentralityResult,
    ComponentResult,
    MaxFlowEdge,
    MaxFlowResult,
    build_graph_from_db,
    NETWORKX_AVAILABLE,
)
from .utils import (
    normalize_hash,
    get_prefix,
    validate_hash,
    validate_prefix,
    make_edge_key,
    parse_edge_key,
    hashes_match,
    sanitize_for_sql,
)
from .errors import (
    ErrorCode,
    AnalyticsError,
    api_success,
    api_error,
    api_error_from_exception,
    invalid_param,
    missing_param,
    not_found,
)
from .config import Config, reload_config
from .validation import (
    ValidationError,
    validate_positive_int,
    validate_positive_float,
    validate_hours,
    validate_limit,
    validate_min_certainty,
    validate_hash_param,
    validate_bool,
    validate_string_choice,
)
from .cache import (
    GraphCache,
    CacheStats,
    get_graph_cache,
    get_cached_graph,
    invalidate_graph_cache,
    get_cache_stats,
)

__all__ = [
    # Edge builder
    "extract_edges_from_packet",
    "EdgeObservation",
    # Bucketing
    "compute_time_bucket",
    "BucketedStats",
    "UtilBucket",
    "TIME_RANGE_PRESETS",
    "TIME_RANGE_OPTIONS",
    "parse_time_range",
    "get_available_presets",
    "get_airtime_utilization_preset",
    # Node activity
    "update_node_activity",
    "ActivityBucket",
    # Topology
    "TopologySnapshot",
    "TopologyEdge",
    # Disambiguation
    "DisambiguationCandidate",
    "DisambiguationResult",
    "DisambiguationStats",
    "PrefixLookup",
    # Path registry
    "ObservedPath",
    "PathHealth",
    # Worker
    "AnalyticsWorker",
    # Database
    "AnalyticsDB",
    "AnalyticsDBError",
    "ConnectionError",
    "QueryError",
    "DBConnection",
    "ensure_connection",
    "with_connection",
    # Graph
    "MeshGraph",
    "PathResult",
    "CentralityResult",
    "ComponentResult",
    "MaxFlowEdge",
    "MaxFlowResult",
    "build_graph_from_db",
    "NETWORKX_AVAILABLE",
    # Utils
    "normalize_hash",
    "get_prefix",
    "validate_hash",
    "validate_prefix",
    "make_edge_key",
    "parse_edge_key",
    "hashes_match",
    "sanitize_for_sql",
    # Errors
    "ErrorCode",
    "AnalyticsError",
    "api_success",
    "api_error",
    "api_error_from_exception",
    "invalid_param",
    "missing_param",
    "not_found",
    # Config
    "Config",
    "reload_config",
    # Validation
    "ValidationError",
    "validate_positive_int",
    "validate_positive_float",
    "validate_hours",
    "validate_limit",
    "validate_min_certainty",
    "validate_hash_param",
    "validate_bool",
    "validate_string_choice",
    # Cache
    "GraphCache",
    "CacheStats",
    "get_graph_cache",
    "get_cached_graph",
    "invalidate_graph_cache",
    "get_cache_stats",
]
