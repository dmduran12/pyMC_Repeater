"""
Node Activity - Sparkline computation for node activity
========================================================

Tracks per-node packet activity over time for sparkline visualization.
Uses 6-hour buckets over 7 days (28 buckets total per node).

Sparklines provide at-a-glance activity trends in the neighbor list
and topology views, showing which nodes are active vs dormant.

Key Concepts
------------
    Activity Bucket:
        6-hour window containing a packet count for one node.
        Bucket timestamps are aligned to 6-hour boundaries
        (00:00, 06:00, 12:00, 18:00 UTC).
        
    Sparkline:
        Array of 28 buckets (7 days) showing activity trend.
        Rendered as a mini bar chart next to each node.
        
    Node Hash:
        2-character prefix identifying a node (e.g., "AB").
        Stored as uppercase for consistency.

Bucket Configuration
--------------------
    BUCKET_HOURS = 6           # Each bucket spans 6 hours
    MAX_BUCKETS = 28           # 7 days of history
    BUCKET_SECONDS = 21600     # 6 * 3600
    SEVEN_DAYS_SECONDS = 604800

Node Extraction
---------------
For each packet, we extract nodes from:

    1. Forwarding path (original_path or forwarded_path)
    2. Source hash (src_hash field)

All extracted nodes get their activity bucket incremented.

Database Schema
---------------
    node_activity:
        - node_hash: 2-char prefix (e.g., "AB")
        - bucket_start: Unix timestamp aligned to 6hr boundary
        - packet_count: Number of packets in this bucket
        - UNIQUE(node_hash, bucket_start)

Public Functions
----------------
    update_node_activity(conn, packet, now)
        Increment activity for all nodes in a packet.
        Called by worker.on_packet_received().
        
    get_sparkline_data(conn, node_hashes, hours, now)
        Get sparklines for specific nodes.
        
    get_all_sparklines(conn, hours, now)
        Get sparklines for ALL active nodes. Used by API.
        
    cleanup_old_activity(conn, days)
        Remove activity data older than N days.

API Endpoint
------------
    GET /api/analytics/sparklines?hours=168
    
    Response:
    {
        "success": true,
        "data": {
            "hours": 168,
            "bucket_hours": 6,
            "sparklines": {
                "0xAB": [{"idx": 0, "count": 12, "timestamp": 1704412800000}, ...],
                "0xCD": [...]
            }
        }
    }

See Also
--------
    - worker.py: Calls update_node_activity() per packet
    - analytics_api.py: sparklines endpoint implementation
"""

import json
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from .edge_builder import parse_path, get_hash_prefix

logger = logging.getLogger("Analytics.NodeActivity")

# Sparkline constants (match frontend)
BUCKET_HOURS = 6
MAX_BUCKETS = 28  # 7 days
BUCKET_SECONDS = BUCKET_HOURS * 3600
SEVEN_DAYS_SECONDS = 7 * 24 * 3600


@dataclass
class ActivityBucket:
    """Single bucket of node activity."""
    bucket_idx: int
    bucket_start: int  # Unix timestamp (seconds)
    packet_count: int = 0
    
    def to_dict(self) -> dict:
        return {
            "idx": self.bucket_idx,
            "count": self.packet_count,
            "timestamp": self.bucket_start * 1000,  # Convert to ms for frontend
        }


def compute_activity_bucket(timestamp: float, display_start: int) -> int:
    """
    Compute bucket index for a timestamp.
    
    Args:
        timestamp: Packet timestamp (seconds)
        display_start: Start of 7-day window (seconds)
        
    Returns:
        Bucket index (0 to MAX_BUCKETS-1), or -1 if outside range
    """
    if timestamp < display_start:
        return -1
    
    bucket_idx = int((timestamp - display_start) / BUCKET_SECONDS)
    
    if bucket_idx >= MAX_BUCKETS:
        return -1
    
    return bucket_idx


def extract_nodes_from_packet(packet: dict) -> Set[str]:
    """
    Extract all node hashes involved in a packet.
    
    Includes:
        - Nodes in the forwarding path
        - Source hash (if available)
        
    Args:
        packet: Packet record dict
        
    Returns:
        Set of node prefix strings (e.g., {"AB", "CD", "EF"})
    """
    nodes = set()
    
    # Get path nodes
    raw_path = packet.get("forwarded_path") or packet.get("original_path")
    path = parse_path(raw_path)
    
    if path:
        for hop in path:
            if hop:
                nodes.add(hop.upper())
    
    # Add source hash
    src_hash = packet.get("src_hash")
    if src_hash:
        prefix = get_hash_prefix(src_hash)
        if prefix:
            nodes.add(prefix)
    
    return nodes


def update_node_activity(
    conn,
    packet: dict,
    now: Optional[float] = None,
) -> int:
    """
    Update node_activity table for a single packet.
    
    Extracts all nodes from packet path and increments their activity counts.
    
    Args:
        conn: SQLite connection
        packet: Packet record dict
        now: Current timestamp (defaults to time.time())
        
    Returns:
        Number of node buckets updated
    """
    if now is None:
        now = time.time()
    
    display_start = int(now) - SEVEN_DAYS_SECONDS
    
    timestamp = packet.get("timestamp", now)
    bucket_idx = compute_activity_bucket(timestamp, display_start)
    
    if bucket_idx < 0:
        return 0  # Outside display window
    
    nodes = extract_nodes_from_packet(packet)
    if not nodes:
        return 0
    
    bucket_start = display_start + (bucket_idx * BUCKET_SECONDS)
    
    cursor = conn.cursor()
    updated = 0
    
    for node_prefix in nodes:
        try:
            # Upsert node activity
            cursor.execute("""
                INSERT INTO node_activity (node_hash, bucket_start, packet_count)
                VALUES (?, ?, 1)
                ON CONFLICT(node_hash, bucket_start) DO UPDATE SET
                    packet_count = packet_count + 1
            """, (node_prefix, bucket_start))
            updated += 1
        except Exception as e:
            logger.error(f"Failed to update activity for {node_prefix}: {e}")
    
    conn.commit()
    return updated


def get_sparkline_data(
    conn,
    node_hashes: List[str],
    hours: int = 168,  # 7 days
    now: Optional[float] = None,
) -> Dict[str, List[ActivityBucket]]:
    """
    Get sparkline data for specific nodes.
    
    Args:
        conn: SQLite connection
        node_hashes: List of full node hashes to query
        hours: Time range in hours (default 7 days)
        now: Current timestamp (defaults to time.time())
        
    Returns:
        Dict mapping node hash -> list of ActivityBucket
    """
    if now is None:
        now = time.time()
    
    display_start = int(now) - (hours * 3600)
    
    # Convert hashes to prefixes for lookup
    prefix_to_hash = {}
    for full_hash in node_hashes:
        prefix = get_hash_prefix(full_hash)
        if prefix:
            prefix_to_hash[prefix] = full_hash
    
    if not prefix_to_hash:
        return {}
    
    cursor = conn.cursor()
    
    # Query activity for all prefixes
    placeholders = ",".join("?" * len(prefix_to_hash))
    cursor.execute(f"""
        SELECT node_hash, bucket_start, packet_count
        FROM node_activity
        WHERE node_hash IN ({placeholders})
          AND bucket_start >= ?
        ORDER BY node_hash, bucket_start
    """, (*prefix_to_hash.keys(), display_start))
    
    # Organize by node hash
    result = {h: [] for h in node_hashes}
    bucket_data = {}  # prefix -> bucket_start -> count
    
    for row in cursor.fetchall():
        prefix = row[0]
        bucket_start = row[1]
        count = row[2]
        
        if prefix not in bucket_data:
            bucket_data[prefix] = {}
        bucket_data[prefix][bucket_start] = count
    
    # Build sparkline arrays
    for prefix, full_hash in prefix_to_hash.items():
        if prefix not in bucket_data:
            continue
        
        # Find earliest activity to avoid empty left padding
        earliest_bucket = min(bucket_data[prefix].keys()) if bucket_data[prefix] else display_start
        start_bucket_idx = max(0, (earliest_bucket - display_start) // BUCKET_SECONDS)
        
        buckets = []
        for i in range(start_bucket_idx, MAX_BUCKETS):
            bucket_start = display_start + (i * BUCKET_SECONDS)
            count = bucket_data[prefix].get(bucket_start, 0)
            
            buckets.append(ActivityBucket(
                bucket_idx=i - start_bucket_idx,
                bucket_start=bucket_start,
                packet_count=count,
            ))
        
        result[full_hash] = buckets
    
    return result


def get_all_sparklines(
    conn,
    hours: int = 168,
    now: Optional[float] = None,
) -> Dict[str, List[dict]]:
    """
    Get sparkline data for ALL nodes with activity.
    
    More efficient than querying individual nodes - returns all
    nodes with any activity in the time window.
    
    Args:
        conn: SQLite connection
        hours: Time range in hours
        now: Current timestamp
        
    Returns:
        Dict mapping node prefix -> list of bucket dicts
    """
    if now is None:
        now = time.time()
    
    display_start = int(now) - (hours * 3600)
    
    cursor = conn.cursor()
    
    # Query all activity in range
    cursor.execute("""
        SELECT node_hash, bucket_start, packet_count
        FROM node_activity
        WHERE bucket_start >= ?
        ORDER BY node_hash, bucket_start
    """, (display_start,))
    
    # Organize by node
    node_buckets = {}
    
    for row in cursor.fetchall():
        prefix = row[0]
        bucket_start = row[1]
        count = row[2]
        
        if prefix not in node_buckets:
            node_buckets[prefix] = {}
        node_buckets[prefix][bucket_start] = count
    
    # Build result with bucket arrays
    result = {}
    
    for prefix, buckets in node_buckets.items():
        if not buckets:
            continue
        
        # Find earliest activity
        earliest_bucket = min(buckets.keys())
        start_bucket_idx = max(0, (earliest_bucket - display_start) // BUCKET_SECONDS)
        
        bucket_list = []
        for i in range(start_bucket_idx, MAX_BUCKETS):
            bucket_start = display_start + (i * BUCKET_SECONDS)
            count = buckets.get(bucket_start, 0)
            
            bucket_list.append({
                "idx": i - start_bucket_idx,
                "count": count,
                "timestamp": bucket_start * 1000,
            })
        
        result[f"0x{prefix}"] = bucket_list
    
    return result


def cleanup_old_activity(conn, days: int = 8):
    """
    Remove activity data older than N days.
    
    Args:
        conn: SQLite connection
        days: Age threshold in days
    """
    cutoff = int(time.time()) - (days * 24 * 3600)
    
    cursor = conn.cursor()
    cursor.execute("DELETE FROM node_activity WHERE bucket_start < ?", (cutoff,))
    deleted = cursor.rowcount
    conn.commit()
    
    if deleted > 0:
        logger.info(f"Cleaned up {deleted} old activity records")
