"""
Path Registry - Path tracking and health metrics
=================================================

Tracks observed packet paths through the mesh network and computes
health indicators for monitoring network stability.

This module answers questions like:
    - What are the most common routes through the mesh?
    - Is a particular path healthy or degrading?
    - Which paths go through hub nodes?
    - What's the weakest link in a path?

Key Concepts
------------
    Observed Path:
        A specific sequence of hops from source to destination.
        Stored as a JSON-encoded list of prefixes (path_signature).
        Example: '["AB", "CD", "EF"]' for a 3-hop path.
        
    Canonical Path:
        The most frequently used path for a source/destination pair.
        Multiple paths may exist between endpoints; canonical is most common.
        
    Path Signature:
        JSON string uniquely identifying a path: '["AB", "CD", "EF"]'
        Used as database key for deduplication.
        
    Path Health:
        Composite score (0-1) based on:
        - Observation count (30%): More observations = more reliable
        - Recency (25%): Recent use = still working
        - Edge confidence (30%): Weak links lower health
        - Hub connectivity (15%): Paths through hubs = better

Health Scoring Formula
----------------------
    health = (
        obs_score * 0.30 +      # Log of observation count
        recency_score * 0.25 +  # Decay over 7 days
        edge_score * 0.30 +     # Min edge confidence in path
        hub_score * 0.15        # Bonus for hub connectivity
    )

Database Schema
---------------
    path_observations:
        - src_hash: Source node hash
        - dst_hash: Destination node hash (nullable)
        - path_signature: JSON array of hop prefixes
        - hop_count: Number of hops in path
        - observation_count: Times this exact path was seen
        - first_seen, last_seen: Timestamps
        - UNIQUE(src_hash, dst_hash, path_signature)

Public Functions
----------------
    record_observed_path(conn, packet, local_hash)
        Record a path observation from a packet.
        Called by worker.on_packet_received().
        
    get_canonical_paths(conn, limit, min_observations)
        Get most frequently used paths.
        
    calculate_path_health(conn, edges, hub_nodes, limit)
        Compute health metrics for top paths.
        
    get_paths_for_node(conn, node_hash, as_source, limit)
        Get paths involving a specific node.
        
    cleanup_old_paths(conn, days)
        Remove path observations older than N days.

API Endpoints
-------------
    GET /api/analytics/paths?limit=100&min_observations=5
        Returns list of ObservedPath objects.
        
    GET /api/analytics/path_health?limit=20
        Returns list of PathHealth objects with scores.

See Also
--------
    - worker.py: Calls record_observed_path() per packet
    - topology.py: Hub nodes used for health calculation
    - analytics_api.py: paths and path_health endpoints
"""

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from .edge_builder import parse_path, make_edge_key

logger = logging.getLogger("Analytics.PathRegistry")


@dataclass
class ObservedPath:
    """A single observed path through the network."""
    src_hash: str
    dst_hash: Optional[str]
    path_signature: str  # JSON array of prefixes
    hop_count: int
    observation_count: int = 1
    first_seen: float = 0.0
    last_seen: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            "srcHash": self.src_hash,
            "dstHash": self.dst_hash,
            "pathSignature": self.path_signature,
            "hopCount": self.hop_count,
            "observationCount": self.observation_count,
            "firstSeen": self.first_seen,
            "lastSeen": self.last_seen,
        }


@dataclass
class PathHealth:
    """Health metrics for an observed path."""
    path_signature: str
    src_hash: str
    dst_hash: Optional[str]
    hop_count: int
    observation_count: int
    health_score: float  # 0-1, higher is better
    observation_trend: float  # Positive = growing, negative = declining
    weakest_edge: Optional[str] = None
    weakest_edge_confidence: float = 1.0
    last_seen: float = 0.0
    alternate_count: int = 0  # Number of alternate paths to same destination
    estimated_latency_ms: Optional[float] = None  # Estimated latency based on hop count
    has_backbone: bool = False  # Whether path uses backbone edges
    
    def to_dict(self) -> dict:
        return {
            "pathSignature": self.path_signature,
            "srcHash": self.src_hash,
            "dstHash": self.dst_hash,
            "hopCount": self.hop_count,
            "observationCount": self.observation_count,
            "healthScore": round(self.health_score, 3),
            "observationTrend": round(self.observation_trend, 3),
            "weakestEdge": self.weakest_edge,
            "weakestEdgeConfidence": round(self.weakest_edge_confidence, 3),
            "lastSeen": self.last_seen,
            "alternateCount": self.alternate_count,
            "estimatedLatencyMs": round(self.estimated_latency_ms) if self.estimated_latency_ms else None,
            "hasBackbone": self.has_backbone,
        }


def make_path_signature(path: List[str]) -> str:
    """
    Create canonical path signature from hop list.
    
    Args:
        path: List of hop prefixes
        
    Returns:
        JSON string of uppercase prefixes
    """
    normalized = [p.upper() for p in path if p]
    return json.dumps(normalized)


def record_observed_path(
    conn,
    packet: dict,
    local_hash: Optional[str] = None,
) -> bool:
    """
    Record an observed path from a packet.
    
    Args:
        conn: SQLite connection
        packet: Packet record dict
        local_hash: Local node's hash
        
    Returns:
        True if path was recorded
    """
    raw_path = packet.get("forwarded_path") or packet.get("original_path")
    path = parse_path(raw_path)
    
    if not path:
        return False
    
    timestamp = packet.get("timestamp", time.time())
    src_hash = packet.get("src_hash")
    dst_hash = packet.get("dst_hash")
    
    path_sig = make_path_signature(path)
    hop_count = len(path)
    
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO path_observations (
                src_hash, dst_hash, path_signature, hop_count,
                observation_count, first_seen, last_seen
            ) VALUES (?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(src_hash, dst_hash, path_signature) DO UPDATE SET
                observation_count = observation_count + 1,
                last_seen = excluded.last_seen
        """, (
            src_hash or "",
            dst_hash,
            path_sig,
            hop_count,
            timestamp,
            timestamp,
        ))
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"Failed to record path: {e}")
        return False


def get_canonical_paths(
    conn,
    limit: int = 100,
    min_observations: int = 5,
) -> List[ObservedPath]:
    """
    Get most frequently observed paths.
    
    Args:
        conn: SQLite connection
        limit: Maximum paths to return
        min_observations: Minimum observation count to include
        
    Returns:
        List of ObservedPath sorted by observation count
    """
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            src_hash, dst_hash, path_signature, hop_count,
            observation_count, first_seen, last_seen
        FROM path_observations
        WHERE observation_count >= ?
        ORDER BY observation_count DESC
        LIMIT ?
    """, (min_observations, limit))
    
    paths = []
    for row in cursor.fetchall():
        paths.append(ObservedPath(
            src_hash=row[0],
            dst_hash=row[1],
            path_signature=row[2],
            hop_count=row[3],
            observation_count=row[4],
            first_seen=row[5],
            last_seen=row[6],
        ))
    
    return paths


# Estimated latency per hop (milliseconds)
# Based on typical LoRa mesh timing including TX/RX turnaround
LATENCY_PER_HOP_MS = 250


def calculate_path_health(
    conn,
    edges: List[dict],
    hub_nodes: List[str],
    backbone_edges: Optional[List[str]] = None,
    limit: int = 20,
) -> List[PathHealth]:
    """
    Calculate health metrics for top observed paths.
    
    Health score factors:
        - Observation count (more = healthier)
        - Recency (recent = healthier)
        - Edge confidence (weak edges = lower health)
        - Hub connectivity (paths through hubs = healthier)
    
    Args:
        conn: SQLite connection
        edges: List of edge dicts for confidence lookup
        hub_nodes: List of hub node hashes
        backbone_edges: Optional list of backbone edge keys
        limit: Maximum paths to analyze
        
    Returns:
        List of PathHealth sorted by health score
    """
    # Build edge confidence lookup
    edge_confidence = {}
    for e in edges:
        edge_key = e.get("edgeKey") or e.get("edge_key")
        conf = e.get("avgConfidence") or e.get("avg_confidence", 0)
        if edge_key:
            edge_confidence[edge_key] = conf
    
    hub_set = set(hub_nodes)
    backbone_set = set(backbone_edges or [])
    
    # Get top paths
    canonical = get_canonical_paths(conn, limit=limit * 2, min_observations=3)
    
    now = time.time()
    health_results = []
    
    for path in canonical:
        try:
            hops = json.loads(path.path_signature)
        except json.JSONDecodeError:
            continue
        
        if len(hops) < 2:
            continue
        
        # Calculate health factors
        
        # 1. Observation count score (logarithmic)
        import math
        obs_score = min(1.0, math.log10(path.observation_count + 1) / 2)
        
        # 2. Recency score (decay over 7 days)
        hours_ago = (now - path.last_seen) / 3600
        recency_score = max(0, 1 - (hours_ago / 168))
        
        # 3. Edge confidence (find weakest link) and backbone detection
        weakest_edge = None
        weakest_conf = 1.0
        has_backbone = False
        
        for i in range(len(hops) - 1):
            from_h = f"0x{hops[i]}"
            to_h = f"0x{hops[i + 1]}"
            edge_key = make_edge_key(from_h, to_h)
            
            # Check backbone
            if edge_key in backbone_set:
                has_backbone = True
            
            conf = edge_confidence.get(edge_key, 0.5)
            if conf < weakest_conf:
                weakest_conf = conf
                weakest_edge = edge_key
        
        edge_score = weakest_conf
        
        # 4. Hub connectivity bonus
        hub_count = sum(1 for h in hops if f"0x{h}" in hub_set)
        hub_score = min(1.0, hub_count * 0.2)
        
        # 5. Estimate latency based on hop count
        estimated_latency = len(hops) * LATENCY_PER_HOP_MS
        
        # Combined health score
        health_score = (
            obs_score * 0.3 +
            recency_score * 0.25 +
            edge_score * 0.3 +
            hub_score * 0.15
        )
        
        # Calculate observation trend
        # Simplified: compare recent vs older observations
        age_hours = (now - path.first_seen) / 3600
        if age_hours > 24 and path.observation_count > 5:
            # Positive trend if observations per day is increasing
            days_active = max(1, age_hours / 24)
            obs_per_day = path.observation_count / days_active
            # Compare to expected baseline
            trend = (obs_per_day - 1) / 5  # Normalize around 1/day
            trend = max(-1, min(1, trend))
        else:
            trend = 0.0
        
        health_results.append(PathHealth(
            path_signature=path.path_signature,
            src_hash=path.src_hash,
            dst_hash=path.dst_hash,
            hop_count=path.hop_count,
            observation_count=path.observation_count,
            health_score=health_score,
            observation_trend=trend,
            weakest_edge=weakest_edge,
            weakest_edge_confidence=weakest_conf,
            last_seen=path.last_seen,
            alternate_count=0,  # Will be calculated below
            estimated_latency_ms=estimated_latency,
            has_backbone=has_backbone,
        ))
    
    # Calculate alternate paths for each source/destination pair
    cursor = conn.cursor()
    for health in health_results:
        if health.src_hash and health.dst_hash:
            cursor.execute("""
                SELECT COUNT(*) FROM path_observations
                WHERE src_hash = ? AND dst_hash = ?
                  AND path_signature != ?
            """, (health.src_hash, health.dst_hash, health.path_signature))
            health.alternate_count = cursor.fetchone()[0]
    
    # Sort by health score descending
    health_results.sort(key=lambda x: x.health_score, reverse=True)
    
    return health_results[:limit]


def get_paths_for_node(
    conn,
    node_hash: str,
    as_source: bool = True,
    limit: int = 50,
) -> List[ObservedPath]:
    """
    Get paths involving a specific node.
    
    Args:
        conn: SQLite connection
        node_hash: Node hash to filter by
        as_source: If True, find paths from this node; if False, to this node
        limit: Maximum paths to return
        
    Returns:
        List of ObservedPath
    """
    cursor = conn.cursor()
    
    if as_source:
        cursor.execute("""
            SELECT 
                src_hash, dst_hash, path_signature, hop_count,
                observation_count, first_seen, last_seen
            FROM path_observations
            WHERE src_hash = ?
            ORDER BY observation_count DESC
            LIMIT ?
        """, (node_hash, limit))
    else:
        cursor.execute("""
            SELECT 
                src_hash, dst_hash, path_signature, hop_count,
                observation_count, first_seen, last_seen
            FROM path_observations
            WHERE dst_hash = ?
            ORDER BY observation_count DESC
            LIMIT ?
        """, (node_hash, limit))
    
    paths = []
    for row in cursor.fetchall():
        paths.append(ObservedPath(
            src_hash=row[0],
            dst_hash=row[1],
            path_signature=row[2],
            hop_count=row[3],
            observation_count=row[4],
            first_seen=row[5],
            last_seen=row[6],
        ))
    
    return paths


def cleanup_old_paths(conn, days: int = 14):
    """
    Remove path observations older than N days.
    
    Args:
        conn: SQLite connection
        days: Age threshold in days
    """
    cutoff = time.time() - (days * 24 * 3600)
    
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM path_observations WHERE last_seen < ?",
        (cutoff,)
    )
    deleted = cursor.rowcount
    conn.commit()
    
    if deleted > 0:
        logger.info(f"Cleaned up {deleted} old path observations")
