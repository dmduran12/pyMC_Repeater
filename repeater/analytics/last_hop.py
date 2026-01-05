"""
Last-Hop Neighbor Detection
===========================

Identifies direct RF neighbors by analyzing which nodes appear as the
final hop (last element) in received packet paths. These are the nodes
that directly forwarded packets to the local node via RF.

Key Concepts
------------
    Last-Hop:
        The last prefix in a packet's forwarding path before reaching
        the local node. This node has direct RF connectivity to us.
        
    ADVERT Packets:
        Type 4 packets that announce node presence. Particularly useful
        for neighbor detection because they include RSSI/SNR data.
        
    Status Classification:
        - ACTIVE: Seen within last 2 hours
        - STALE: Seen within 2-24 hours
        - EXPIRED: Not seen in >24 hours

Neighbor Scoring
----------------
Each last-hop neighbor is scored based on:
    1. Frequency: How often they forward packets to us
    2. Recency: How recently we've seen them
    3. Signal Quality: Average RSSI/SNR from ADVERT packets
    4. Consistency: How reliable they are as a forwarder

API Response Format
-------------------
    GET /api/analytics/last_hop_neighbors
    
    Returns:
    {
        "success": true,
        "data": {
            "neighbors": [
                {
                    "hash": "0xABCD1234",
                    "prefix": "AB",
                    "count": 150,
                    "confidence": 0.95,
                    "avgRssi": -85,
                    "avgSnr": 8.5,
                    "lastSeen": 1704067200,
                    "status": "active"
                }
            ],
            "totalPackets": 5000,
            "uniqueLastHops": 12,
            "activeNeighbors": 8
        }
    }

See Also
--------
    - disambiguation.py: Resolves prefix collisions for last-hop prefixes
    - edge_builder.py: Uses last-hop for edge extraction
"""

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .edge_builder import get_hash_prefix, parse_path
from .disambiguation import (
    build_prefix_lookup,
    resolve_prefix,
    PrefixLookup,
    ResolutionContext,
)

logger = logging.getLogger("Analytics.LastHop")

# Status thresholds (in hours)
ACTIVE_THRESHOLD_HOURS = 2
STALE_THRESHOLD_HOURS = 24

# Neighbor status constants
STATUS_ACTIVE = "active"
STATUS_STALE = "stale"
STATUS_EXPIRED = "expired"


@dataclass
class LastHopNeighbor:
    """A node identified as a direct RF neighbor (last-hop forwarder)."""
    hash: Optional[str]
    prefix: str
    count: int = 0
    confidence: float = 0.0
    avg_rssi: Optional[float] = None
    avg_snr: Optional[float] = None
    last_seen: float = 0.0
    status: str = STATUS_EXPIRED
    
    # Internal tracking
    rssi_sum: float = 0.0
    rssi_count: int = 0
    snr_sum: float = 0.0
    snr_count: int = 0
    
    def to_dict(self) -> dict:
        return {
            "hash": self.hash,
            "prefix": self.prefix,
            "count": self.count,
            "confidence": round(self.confidence, 3),
            "avgRssi": round(self.avg_rssi, 1) if self.avg_rssi is not None else None,
            "avgSnr": round(self.avg_snr, 1) if self.avg_snr is not None else None,
            "lastSeen": self.last_seen,
            "status": self.status,
        }


@dataclass
class LastHopStats:
    """Statistics about last-hop neighbor analysis."""
    neighbors: List[LastHopNeighbor]
    total_packets: int = 0
    unique_last_hops: int = 0
    active_neighbors: int = 0
    stale_neighbors: int = 0
    expired_neighbors: int = 0
    avg_confidence: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            "neighbors": [n.to_dict() for n in self.neighbors],
            "totalPackets": self.total_packets,
            "uniqueLastHops": self.unique_last_hops,
            "activeNeighbors": self.active_neighbors,
            "staleNeighbors": self.stale_neighbors,
            "expiredNeighbors": self.expired_neighbors,
            "avgConfidence": round(self.avg_confidence, 3),
        }


def get_neighbor_status(last_seen: float, now: Optional[float] = None) -> str:
    """
    Determine neighbor status based on when last seen.
    
    Args:
        last_seen: Unix timestamp of last observation
        now: Current timestamp (defaults to time.time())
        
    Returns:
        Status string: "active", "stale", or "expired"
    """
    now = now or time.time()
    hours_ago = (now - last_seen) / 3600
    
    if hours_ago < ACTIVE_THRESHOLD_HOURS:
        return STATUS_ACTIVE
    elif hours_ago < STALE_THRESHOLD_HOURS:
        return STATUS_STALE
    return STATUS_EXPIRED


def get_last_hop_neighbors(
    conn,
    neighbors: Dict[str, dict],
    local_hash: Optional[str] = None,
    local_lat: Optional[float] = None,
    local_lon: Optional[float] = None,
    hours: int = 168,
) -> LastHopStats:
    """
    Identify direct RF neighbors from packet last-hops.
    
    Analyzes the last element of packet paths to find nodes that
    directly forward packets to the local node.
    
    Args:
        conn: SQLite connection
        neighbors: Dict of known neighbors for disambiguation
        local_hash: Local node's hash
        local_lat: Local node's latitude (for disambiguation)
        local_lon: Local node's longitude (for disambiguation)
        hours: Time range to analyze (default: 168 = 7 days)
        
    Returns:
        LastHopStats with all detected last-hop neighbors
    """
    now = time.time()
    cutoff = now - (hours * 3600)
    
    # Build disambiguation lookup
    lookup = build_prefix_lookup(
        conn,
        neighbors,
        local_hash,
        local_lat,
        local_lon,
        hours=hours,
    )
    
    # Track last-hop observations by prefix
    prefix_stats: Dict[str, LastHopNeighbor] = {}
    total_packets = 0
    local_prefix = get_hash_prefix(local_hash) if local_hash else None
    
    cursor = conn.cursor()
    
    # Query all packets with paths
    cursor.execute("""
        SELECT timestamp, original_path, forwarded_path, rssi, snr, type
        FROM packets
        WHERE timestamp > ?
          AND (original_path IS NOT NULL OR forwarded_path IS NOT NULL)
        ORDER BY timestamp
    """, (cutoff,))
    
    for row in cursor.fetchall():
        timestamp = row[0]
        raw_path = row[2] or row[1]  # Prefer forwarded_path
        rssi = row[3]
        snr = row[4]
        pkt_type = row[5]
        
        path = parse_path(raw_path)
        if not path:
            continue
        
        total_packets += 1
        
        # Get last hop prefix
        last_hop_prefix = path[-1].upper()
        
        # Skip if last hop is local (shouldn't happen, but safety check)
        if local_prefix and last_hop_prefix == local_prefix:
            continue
        
        # Get or create stats entry
        if last_hop_prefix not in prefix_stats:
            prefix_stats[last_hop_prefix] = LastHopNeighbor(
                hash=None,
                prefix=last_hop_prefix,
            )
        
        stats = prefix_stats[last_hop_prefix]
        stats.count += 1
        stats.last_seen = max(stats.last_seen, timestamp)
        
        # Track RSSI/SNR from ADVERT packets (type 4) - these have direct signal data
        if pkt_type == 4:
            if rssi is not None:
                stats.rssi_sum += rssi
                stats.rssi_count += 1
            if snr is not None:
                stats.snr_sum += snr
                stats.snr_count += 1
    
    # Resolve prefixes to full hashes and finalize stats
    result_neighbors = []
    
    for prefix, stats in prefix_stats.items():
        # Resolve prefix using disambiguation
        context = ResolutionContext(position=1, is_last_hop=True)
        resolved_hash, confidence = lookup.resolve(prefix, context)
        
        stats.hash = resolved_hash
        stats.confidence = confidence
        
        # Calculate averages
        if stats.rssi_count > 0:
            stats.avg_rssi = stats.rssi_sum / stats.rssi_count
        if stats.snr_count > 0:
            stats.avg_snr = stats.snr_sum / stats.snr_count
        
        # Determine status
        stats.status = get_neighbor_status(stats.last_seen, now)
        
        result_neighbors.append(stats)
    
    # Sort by count descending
    result_neighbors.sort(key=lambda n: n.count, reverse=True)
    
    # Count by status
    active_count = sum(1 for n in result_neighbors if n.status == STATUS_ACTIVE)
    stale_count = sum(1 for n in result_neighbors if n.status == STATUS_STALE)
    expired_count = sum(1 for n in result_neighbors if n.status == STATUS_EXPIRED)
    
    # Calculate average confidence
    confidences = [n.confidence for n in result_neighbors if n.confidence > 0]
    avg_conf = sum(confidences) / len(confidences) if confidences else 0
    
    return LastHopStats(
        neighbors=result_neighbors,
        total_packets=total_packets,
        unique_last_hops=len(result_neighbors),
        active_neighbors=active_count,
        stale_neighbors=stale_count,
        expired_neighbors=expired_count,
        avg_confidence=avg_conf,
    )


def get_last_hop_traffic_share(
    conn,
    hours: int = 24,
) -> Dict[str, float]:
    """
    Calculate traffic share percentage for each last-hop prefix.
    
    Useful for identifying dominant gateway nodes.
    
    Args:
        conn: SQLite connection
        hours: Time range to analyze
        
    Returns:
        Dict mapping prefix -> percentage of total last-hop traffic
    """
    cutoff = time.time() - (hours * 3600)
    cursor = conn.cursor()
    
    # Count last-hop occurrences
    cursor.execute("""
        SELECT original_path, forwarded_path
        FROM packets
        WHERE timestamp > ?
          AND (original_path IS NOT NULL OR forwarded_path IS NOT NULL)
    """, (cutoff,))
    
    prefix_counts: Dict[str, int] = defaultdict(int)
    total = 0
    
    for row in cursor.fetchall():
        raw_path = row[1] or row[0]
        path = parse_path(raw_path)
        if not path:
            continue
        
        last_hop = path[-1].upper()
        prefix_counts[last_hop] += 1
        total += 1
    
    if total == 0:
        return {}
    
    # Convert to percentages
    return {
        prefix: (count / total) * 100
        for prefix, count in prefix_counts.items()
    }


def identify_gateway_nodes(
    conn,
    neighbors: Dict[str, dict],
    local_hash: Optional[str] = None,
    local_lat: Optional[float] = None,
    local_lon: Optional[float] = None,
    hours: int = 24,
    hub_threshold: float = 15.0,
    gateway_threshold: float = 5.0,
) -> Tuple[List[str], List[str]]:
    """
    Identify hub and gateway nodes based on last-hop traffic.
    
    Hub: >15% of last-hop traffic
    Gateway: 5-15% of last-hop traffic
    
    Args:
        conn: SQLite connection
        neighbors: Known neighbors for disambiguation
        local_hash: Local node's hash
        local_lat: Local latitude
        local_lon: Local longitude
        hours: Time range to analyze
        hub_threshold: Percentage threshold for hub classification
        gateway_threshold: Percentage threshold for gateway classification
        
    Returns:
        Tuple of (hub_hashes, gateway_hashes)
    """
    # Get traffic share
    traffic_share = get_last_hop_traffic_share(conn, hours)
    
    if not traffic_share:
        return [], []
    
    # Build disambiguation lookup
    lookup = build_prefix_lookup(
        conn,
        neighbors,
        local_hash,
        local_lat,
        local_lon,
        hours=hours,
    )
    
    hubs = []
    gateways = []
    
    for prefix, share in traffic_share.items():
        # Resolve to full hash
        context = ResolutionContext(position=1, is_last_hop=True)
        resolved_hash, confidence = lookup.resolve(prefix, context)
        
        if not resolved_hash:
            continue
        
        if share >= hub_threshold:
            hubs.append(resolved_hash)
        elif share >= gateway_threshold:
            gateways.append(resolved_hash)
    
    return hubs, gateways
