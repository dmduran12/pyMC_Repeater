"""
Analytics Worker - Background computation thread
=================================================

Maintains analytics tables by processing packets incrementally
and periodically recomputing aggregate statistics. Runs as a
daemon thread alongside the main repeater process.

This is the "engine" of the analytics system - it ensures all
analytics tables stay current without blocking packet processing.

Architecture
------------
    The worker runs in a separate daemon thread and:
    
    1. **Real-time Processing**: Receives packets via on_packet_received()
       called by StorageCollector after each packet is stored.
       
    2. **Periodic Aggregation**: Every 5 minutes, rebuilds the topology
       snapshot from accumulated edge observations.
       
    3. **Cleanup**: Every hour, removes old data to prevent unbounded
       database growth (configurable retention periods).

Lifecycle
---------
    # Started automatically by storage_collector.py
    storage_collector.start_analytics_worker()
    
    # Or manually
    worker = AnalyticsWorker(sqlite_path, local_hash)
    worker.start()  # Spawns daemon thread
    
    # Shutdown
    worker.stop()  # Signals thread to exit, waits up to 5s

Thread Safety
-------------
    - Each method creates its own SQLite connection (thread-local)
    - No shared mutable state between threads
    - Stop signal uses threading.Event for safe coordination

Data Flow
---------
    1. StorageCollector.record_packet() stores packet to SQLite
    2. StorageCollector calls analytics_worker.on_packet_received()
    3. Worker extracts edges, updates activity, records path
    4. Every 5 min: Worker rebuilds topology_snapshot table
    5. API queries read from precomputed tables (fast)

Configuration
-------------
    DEFAULT_INTERVAL_SECONDS = 60      # Main loop check interval
    TOPOLOGY_REBUILD_INTERVAL = 300    # Rebuild topology every 5 min
    CLEANUP_INTERVAL = 3600            # Cleanup old data every hour

Retention Periods
-----------------
    - node_activity: 8 days (cleanup_old_activity)
    - path_observations: 14 days (cleanup_old_paths)  
    - topology_snapshot: Keep last 10 snapshots
    - edge_observations: No automatic cleanup (persistent)

Public Methods
--------------
    start()
        Start the background worker thread.
        
    stop()
        Signal worker to stop and wait for thread exit.
        
    on_packet_received(packet)
        Process a packet for analytics. Called by StorageCollector.
        
    force_topology_rebuild()
        Trigger immediate topology rebuild (for testing/admin).
        
    get_connection()
        Get a database connection for direct queries.

Example
-------
    >>> worker = AnalyticsWorker(
    ...     sqlite_path='/var/lib/pymc_repeater/packets.db',
    ...     local_hash='AB',
    ...     neighbors_getter=storage.get_neighbors
    ... )
    >>> worker.start()
    >>> # Worker now processes packets in background
    >>> worker.on_packet_received(packet_record)
    >>> # Later...
    >>> worker.stop()

See Also
--------
    - storage_collector.py: Integrates worker with packet storage
    - topology.py: build_topology_from_db() called by worker
    - edge_builder.py: extract_edges_from_packet() called per packet
"""

import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable, Optional, Union

from .db import AnalyticsDB, AnalyticsDBError
from .edge_builder import extract_edges_from_packet, update_edge_observations
from .node_activity import update_node_activity, cleanup_old_activity
from .path_registry import record_observed_path, cleanup_old_paths
from .topology import build_topology_from_db, save_topology_snapshot

logger = logging.getLogger("Analytics.Worker")

# Worker configuration
DEFAULT_INTERVAL_SECONDS = 60
TOPOLOGY_REBUILD_INTERVAL = 300  # 5 minutes
CLEANUP_INTERVAL = 3600  # 1 hour


class AnalyticsWorker:
    """
    Background worker for analytics computation.
    
    Runs in a separate thread, processing packets and maintaining
    aggregate tables. Can be started/stopped with the daemon.
    """
    
    def __init__(
        self,
        sqlite_path: Union[str, Path, AnalyticsDB],
        local_hash: Optional[str] = None,
        neighbors_getter: Optional[Callable[[], dict]] = None,
        interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    ):
        """
        Initialize the analytics worker.
        
        Args:
            sqlite_path: Path to SQLite database, or existing AnalyticsDB instance
            local_hash: Local node's hash (2-char prefix)
            neighbors_getter: Callable that returns current neighbors dict
            interval_seconds: Main loop interval
        """
        # Initialize database connection manager
        if isinstance(sqlite_path, AnalyticsDB):
            self.db = sqlite_path
        else:
            self.db = AnalyticsDB(sqlite_path)
        self.local_hash = local_hash
        self.neighbors_getter = neighbors_getter
        self.interval = interval_seconds
        
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._stop_event = threading.Event()
        
        # Timing trackers
        self._last_topology_rebuild = 0
        self._last_cleanup = 0
        
        # Packet processing queue for batching (reduces DB writes)
        self._packet_queue: list = []
        self._queue_lock = threading.Lock()
        self._max_queue_size = 50  # Flush after 50 packets
        
        # Ensure analytics tables exist
        self._init_tables()
    
    def _init_tables(self):
        """
        Create analytics tables if they don't exist.
        
        These tables are created in the same database as the main packets
        table (repeater.db) to enable efficient JOINs and queries.
        """
        try:
            with self.db.connection() as conn:
                cursor = conn.cursor()
                
                # Edge observations table - tracks topology edges
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS edge_observations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        from_hash TEXT NOT NULL,
                        to_hash TEXT NOT NULL,
                        edge_key TEXT NOT NULL UNIQUE,
                        observation_count INTEGER DEFAULT 0,
                        certain_count INTEGER DEFAULT 0,
                        confidence_sum REAL DEFAULT 0,
                        forward_count INTEGER DEFAULT 0,
                        reverse_count INTEGER DEFAULT 0,
                        first_seen REAL NOT NULL,
                        last_seen REAL NOT NULL
                    )
                """)
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_edge_key ON edge_observations(edge_key)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_edge_certain ON edge_observations(certain_count)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_edge_last_seen ON edge_observations(last_seen)"
                )
                
                # Node activity table - sparkline data (6hr buckets)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS node_activity (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        node_hash TEXT NOT NULL,
                        bucket_start INTEGER NOT NULL,
                        packet_count INTEGER DEFAULT 0,
                        UNIQUE(node_hash, bucket_start)
                    )
                """)
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_activity_hash ON node_activity(node_hash)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_activity_bucket ON node_activity(bucket_start)"
                )
                
                # Path observations table - tracks packet routing paths
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS path_observations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        src_hash TEXT NOT NULL,
                        dst_hash TEXT,
                        path_signature TEXT NOT NULL,
                        hop_count INTEGER NOT NULL,
                        observation_count INTEGER DEFAULT 1,
                        first_seen REAL NOT NULL,
                        last_seen REAL NOT NULL,
                        UNIQUE(src_hash, dst_hash, path_signature)
                    )
                """)
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_path_src ON path_observations(src_hash)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_path_dst ON path_observations(dst_hash)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_path_last_seen ON path_observations(last_seen)"
                )
                
                # Topology snapshot table - precomputed graph analysis
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS topology_snapshot (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        computed_at REAL NOT NULL,
                        local_hash TEXT,
                        edges_json TEXT NOT NULL,
                        hub_nodes_json TEXT NOT NULL,
                        centrality_json TEXT NOT NULL,
                        loops_json TEXT,
                        stats_json TEXT
                    )
                """)
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_topology_computed ON topology_snapshot(computed_at)"
                )
                
                logger.info(f"Analytics tables initialized in {self.db.path}")
                
        except AnalyticsDBError as e:
            logger.error(f"Failed to initialize analytics tables: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing tables: {e}")
            raise
    
    def start(self):
        """Start the background worker thread."""
        if self._running:
            logger.warning("Analytics worker already running")
            return
        
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="AnalyticsWorker",
            daemon=True,
        )
        self._thread.start()
        logger.info("Analytics worker started")
    
    def stop(self):
        """Stop the background worker thread."""
        if not self._running:
            return
        
        self._running = False
        self._stop_event.set()
        
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        
        logger.info("Analytics worker stopped")
    
    def _run_loop(self):
        """Main worker loop."""
        logger.info("Analytics worker loop started")
        
        while not self._stop_event.is_set():
            try:
                now = time.time()
                
                # Periodic topology rebuild
                if now - self._last_topology_rebuild >= TOPOLOGY_REBUILD_INTERVAL:
                    self._rebuild_topology()
                    self._last_topology_rebuild = now
                
                # Periodic cleanup
                if now - self._last_cleanup >= CLEANUP_INTERVAL:
                    self._cleanup_old_data()
                    self._last_cleanup = now
                
            except Exception as e:
                logger.error(f"Error in analytics worker loop: {e}")
            
            # Sleep with interrupt capability
            self._stop_event.wait(self.interval)
        
        logger.info("Analytics worker loop exited")
    
    def on_packet_received(self, packet: dict):
        """
        Process a new packet for analytics.
        
        Called by storage collector when a packet is recorded.
        Updates edge observations, node activity, and path registry.
        
        This method is called from the main thread for each packet,
        so it must be efficient. Heavy processing is deferred to the
        background thread.
        
        Args:
            packet: Packet record dict
        """
        try:
            neighbors = self.neighbors_getter() if self.neighbors_getter else {}
            
            with self.db.connection() as conn:
                # Extract and record edges
                edges = extract_edges_from_packet(
                    packet,
                    local_hash=self.local_hash,
                    neighbors=neighbors,
                )
                if edges:
                    update_edge_observations(conn, edges)
                
                # Update node activity
                update_node_activity(conn, packet)
                
                # Record path
                record_observed_path(conn, packet, self.local_hash)
                
        except AnalyticsDBError as e:
            logger.error(f"Database error processing packet: {e}")
        except Exception as e:
            logger.error(f"Error processing packet for analytics: {e}")
    
    def _rebuild_topology(self):
        """
        Rebuild topology snapshot from current data.
        
        Called periodically by the background thread to update the
        cached topology analysis.
        """
        try:
            with self.db.connection() as conn:
                snapshot = build_topology_from_db(
                    conn,
                    local_hash=self.local_hash,
                )
                
                if snapshot.edges:
                    save_topology_snapshot(conn, snapshot)
                    logger.debug(
                        f"Rebuilt topology: {len(snapshot.edges)} edges, "
                        f"{len(snapshot.hub_nodes)} hubs, "
                        f"{len(snapshot.loops)} loops"
                    )
                else:
                    logger.debug("No edges to build topology from")
                    
        except AnalyticsDBError as e:
            logger.error(f"Database error rebuilding topology: {e}")
        except Exception as e:
            logger.error(f"Error rebuilding topology: {e}", exc_info=True)
    
    def _cleanup_old_data(self):
        """
        Clean up old analytics data to prevent unbounded growth.
        
        Retention periods:
            - node_activity: 8 days
            - path_observations: 14 days
            - topology_snapshot: Keep last 10
        """
        try:
            with self.db.connection() as conn:
                # Clean up old node activity (sparkline data)
                cleanup_old_activity(conn, days=8)
                
                # Clean up old path observations
                cleanup_old_paths(conn, days=14)
                
                # Cleanup old topology snapshots (keep last 10)
                cursor = conn.cursor()
                cursor.execute("""
                    DELETE FROM topology_snapshot
                    WHERE id NOT IN (
                        SELECT id FROM topology_snapshot
                        ORDER BY computed_at DESC
                        LIMIT 10
                    )
                """)
                snapshots_deleted = cursor.rowcount
                
                if snapshots_deleted > 0:
                    logger.info(f"Cleaned up {snapshots_deleted} old topology snapshots")
                
        except AnalyticsDBError as e:
            logger.error(f"Database error during cleanup: {e}")
        except Exception as e:
            logger.error(f"Error cleaning up analytics data: {e}")
    
    def force_topology_rebuild(self):
        """Force an immediate topology rebuild (for testing/admin)."""
        self._rebuild_topology()
    
    def get_connection(self) -> sqlite3.Connection:
        """
        Get a database connection for external queries.
        
        Returns:
            Configured SQLite connection
            
        Note:
            Caller is responsible for closing the connection.
            Prefer using `with db.connection() as conn:` pattern.
        """
        return self.db.get_connection()
    
    @property
    def sqlite_path(self) -> str:
        """Get the database path (for compatibility)."""
        return self.db.path
    
    @property
    def is_running(self) -> bool:
        """Check if worker is running."""
        return self._running
