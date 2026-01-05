"""
Analytics API - CherryPy endpoints for SQL-backed analytics

Provides REST endpoints for:
    - Bucketed airtime statistics
    - Topology graph data
    - Node activity sparklines
    - Path health metrics
    - Disambiguation statistics

All endpoints return JSON responses matching the frontend's expected format.
"""

import logging
import sqlite3
import time
from typing import Optional

import cherrypy

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

logger = logging.getLogger("AnalyticsAPI")


class AnalyticsAPI:
    """
    CherryPy-mounted API for analytics endpoints.
    
    Mount at /api/analytics for URLs like:
        GET /api/analytics/bucketed_stats
        GET /api/analytics/topology
        GET /api/analytics/sparklines
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
    
    def _get_connection(self):
        """Get SQLite connection."""
        return sqlite3.connect(self.sqlite_path)
    
    def _success(self, data, **kwargs):
        """Build success response."""
        result = {"success": True, "data": data}
        result.update(kwargs)
        return result
    
    def _error(self, error):
        """Build error response."""
        return {"success": False, "error": str(error)}
    
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
                    minutes = int(minutes)
                    bucket_count = int(bucket_count)
                    stats = get_airtime_utilization(
                        conn,
                        minutes=minutes,
                        bucket_count=bucket_count,
                        radio_config=radio_config,
                    )
                
                return self._success(stats.to_dict())
                
        except ValueError as e:
            return self._error(f"Invalid parameter: {e}")
        except Exception as e:
            logger.error(f"Error getting bucketed stats: {e}")
            return self._error(e)
    
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
            return self._success({
                "presets": presets,
                "default": "1h",
            })
        except Exception as e:
            logger.error(f"Error getting time presets: {e}")
            return self._error(e)
    
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
            rebuild = str(rebuild).lower() == "true"
            
            with self._get_connection() as conn:
                if rebuild:
                    snapshot = build_topology_from_db(conn, self.local_hash)
                else:
                    snapshot = load_latest_topology(conn)
                    if not snapshot:
                        snapshot = build_topology_from_db(conn, self.local_hash)
                
                return self._success(snapshot.to_dict())
                
        except Exception as e:
            logger.error(f"Error getting topology: {e}")
            return self._error(e)
    
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
            min_certainty = int(min_certainty)
            limit = int(limit)
            
            with self._get_connection() as conn:
                edges = get_validated_edges(
                    conn,
                    min_certain_count=min_certainty,
                    limit=limit,
                )
                
                return self._success(edges, count=len(edges))
                
        except ValueError as e:
            return self._error(f"Invalid parameter: {e}")
        except Exception as e:
            logger.error(f"Error getting topology edges: {e}")
            return self._error(e)
    
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
            hours = int(hours)
            
            with self._get_connection() as conn:
                sparklines = get_all_sparklines(conn, hours=hours)
                
                return self._success({
                    "hours": hours,
                    "bucket_hours": 6,
                    "sparklines": sparklines,
                })
                
        except ValueError as e:
            return self._error(f"Invalid parameter: {e}")
        except Exception as e:
            logger.error(f"Error getting sparklines: {e}")
            return self._error(e)
    
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
            limit = int(limit)
            min_observations = int(min_observations)
            
            with self._get_connection() as conn:
                paths = get_canonical_paths(
                    conn,
                    limit=limit,
                    min_observations=min_observations,
                )
                
                return self._success(
                    [p.to_dict() for p in paths],
                    count=len(paths),
                )
                
        except ValueError as e:
            return self._error(f"Invalid parameter: {e}")
        except Exception as e:
            logger.error(f"Error getting paths: {e}")
            return self._error(e)
    
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
            limit = int(limit)
            
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
                    limit=limit,
                )
                
                return self._success(
                    [h.to_dict() for h in health],
                    count=len(health),
                )
                
        except ValueError as e:
            return self._error(f"Invalid parameter: {e}")
        except Exception as e:
            logger.error(f"Error getting path health: {e}")
            return self._error(e)
    
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
                
                return self._success(stats.to_dict())
                
        except Exception as e:
            logger.error(f"Error getting disambiguation stats: {e}")
            return self._error(e)
    
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
            hours = int(hours)
            
            neighbors = {}
            if self.neighbors_getter:
                neighbors = self.neighbors_getter()
            
            with self._get_connection() as conn:
                stats = get_last_hop_neighbors(
                    conn,
                    neighbors=neighbors,
                    local_hash=self.local_hash,
                    hours=hours,
                )
                
                return self._success(stats.to_dict())
                
        except ValueError as e:
            return self._error(f"Invalid parameter: {e}")
        except Exception as e:
            logger.error(f"Error getting last hop neighbors: {e}")
            return self._error(e)
    
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
                
                return self._success(stats.to_dict())
                
        except ValueError as e:
            return self._error(f"Invalid parameter: {e}")
        except Exception as e:
            logger.error(f"Error getting neighbor affinity: {e}")
            return self._error(e)
    
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
                
                return self._success(recommendation.to_dict())
                
        except ValueError as e:
            return self._error(f"Invalid parameter: {e}")
        except Exception as e:
            logger.error(f"Error getting TX recommendations: {e}")
            return self._error(e)
    
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
                
                return self._success(stats.to_dict())
                
        except ValueError as e:
            return self._error(f"Invalid parameter: {e}")
        except Exception as e:
            logger.error(f"Error detecting mobile nodes: {e}")
            return self._error(e)
    
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
                        return self._success(result.to_dict())
                    else:
                        return self._success({
                            "prefix": prefix.upper(),
                            "candidates": [],
                            "bestMatch": None,
                            "confidence": 0,
                            "isUnambiguous": False,
                        })
                else:
                    return self._success(lookup.get_stats().to_dict())
                
        except ValueError as e:
            return self._error(f"Invalid parameter: {e}")
        except Exception as e:
            logger.error(f"Error getting disambiguation: {e}")
            return self._error(e)
    
    @cherrypy.expose
    def default(self, *args, **kwargs):
        """Handle unmatched routes."""
        if cherrypy.request.method == "OPTIONS":
            return ""
        raise cherrypy.HTTPError(404, "Analytics endpoint not found")
