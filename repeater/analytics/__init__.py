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
    make_edge_key,
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
    PrefixCandidate,
    ResolvedPrefix,
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
)

__all__ = [
    # Edge builder
    "extract_edges_from_packet",
    "make_edge_key",
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
    "PrefixCandidate",
    "ResolvedPrefix",
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
]
