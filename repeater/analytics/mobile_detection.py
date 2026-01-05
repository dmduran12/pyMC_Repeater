"""
Mobile Node Detection
=====================

Identifies nodes that exhibit mobile behavior based on their path patterns.
Mobile nodes (e.g., handheld devices, vehicles) show high path volatility
and shorter active windows compared to fixed repeaters.

Detection Criteria
------------------
    1. Path Volatility (Primary):
       How frequently does the node's forwarding path change?
       Mobile nodes show high volatility as they move through coverage areas.
       
    2. Active Window Ratio:
       What percentage of the observation period is the node active?
       Mobile nodes have sporadic activity patterns.
       
    3. Average Path Lifespan:
       How long do routing paths to this node remain stable?
       Mobile nodes have shorter path lifespans.
       
    4. Position Variance:
       How much does the node's hop position vary across packets?
       Mobile nodes appear at different positions as they move.

Classification Thresholds
-------------------------
    MOBILE_VOLATILITY_THRESHOLD = 0.6 (60%+ path changes)
    MOBILE_ACTIVE_RATIO_THRESHOLD = 0.3 (active <30% of time)
    MOBILE_PATH_LIFESPAN_HOURS = 4 (avg path lasts <4 hours)

API Response Format
-------------------
    GET /api/analytics/mobile_nodes
    
    Returns:
    {
        "success": true,
        "data": {
            "nodes": [
                {
                    "hash": "0xAB123456",
                    "isMobile": true,
                    "volatility": 0.75,
                    "activeRatio": 0.25,
                    "avgPathLifespan": 2.5,
                    "positionVariance": 0.8,
                    "confidence": 0.85
                }
            ],
            "totalNodes": 45,
            "mobileCount": 8,
            "fixedCount": 37
        }
    }

See Also
--------
    - mesh-topology.ts: Frontend mobile detection
    - path_registry.py: Path observation data
"""

import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from .edge_builder import get_hash_prefix, parse_path

logger = logging.getLogger("Analytics.MobileDetection")

# Classification thresholds
MOBILE_VOLATILITY_THRESHOLD = 0.6       # 60%+ path changes
MOBILE_ACTIVE_RATIO_THRESHOLD = 0.3     # Active <30% of observation period
MOBILE_PATH_LIFESPAN_HOURS = 4          # Avg path lasts <4 hours
MOBILE_POSITION_VARIANCE_THRESHOLD = 0.5  # High position variance

# Minimum observations for classification
MIN_OBSERVATIONS = 10


@dataclass
class MobileNodeInfo:
    """Information about a node's mobility characteristics."""
    hash: str
    prefix: str
    is_mobile: bool = False
    volatility: float = 0.0           # 0-1: how often path changes
    active_ratio: float = 0.0         # 0-1: portion of time node is active
    avg_path_lifespan: float = 0.0    # Hours
    position_variance: float = 0.0    # 0-1: variance in hop position
    confidence: float = 0.0           # Confidence in classification
    
    # Observation data
    observation_count: int = 0
    unique_paths: int = 0
    active_hours: int = 0
    total_hours: int = 0
    
    def to_dict(self) -> dict:
        return {
            "hash": self.hash,
            "prefix": self.prefix,
            "isMobile": self.is_mobile,
            "volatility": round(self.volatility, 3),
            "activeRatio": round(self.active_ratio, 3),
            "avgPathLifespan": round(self.avg_path_lifespan, 2),
            "positionVariance": round(self.position_variance, 3),
            "confidence": round(self.confidence, 3),
            "observationCount": self.observation_count,
            "uniquePaths": self.unique_paths,
            "activeHours": self.active_hours,
        }


@dataclass
class MobileDetectionStats:
    """Statistics about mobile node detection."""
    nodes: List[MobileNodeInfo]
    total_nodes: int = 0
    mobile_count: int = 0
    fixed_count: int = 0
    avg_volatility: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            "nodes": [n.to_dict() for n in self.nodes],
            "totalNodes": self.total_nodes,
            "mobileCount": self.mobile_count,
            "fixedCount": self.fixed_count,
            "avgVolatility": round(self.avg_volatility, 3),
        }


def detect_mobile_nodes(
    conn,
    neighbors: Dict[str, dict],
    local_hash: Optional[str] = None,
    hours: int = 168,
    min_observations: int = MIN_OBSERVATIONS,
) -> MobileDetectionStats:
    """
    Detect mobile nodes based on path volatility and activity patterns.
    
    Args:
        conn: SQLite connection
        neighbors: Dict of known neighbors
        local_hash: Local node's hash
        hours: Time range to analyze
        min_observations: Minimum observations for classification
        
    Returns:
        MobileDetectionStats with all node classifications
    """
    now = time.time()
    cutoff = now - (hours * 3600)
    
    # Track per-node statistics
    node_stats: Dict[str, Dict] = defaultdict(lambda: {
        "paths": [],
        "path_timestamps": [],
        "positions": [],
        "active_hours": set(),
    })
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT timestamp, src_hash, original_path, forwarded_path
        FROM packets
        WHERE timestamp > ?
          AND (original_path IS NOT NULL OR forwarded_path IS NOT NULL)
        ORDER BY timestamp
    """, (cutoff,))
    
    for row in cursor.fetchall():
        timestamp = row[0]
        src_hash = row[1]
        raw_path = row[3] or row[2]
        
        path = parse_path(raw_path)
        if not path:
            continue
        
        # Track source node
        if src_hash:
            stats = node_stats[src_hash]
            path_key = ",".join(path)
            stats["paths"].append(path_key)
            stats["path_timestamps"].append(timestamp)
            stats["positions"].append(len(path))  # Source is at start
            
            hour_bucket = int(timestamp // 3600)
            stats["active_hours"].add(hour_bucket)
        
        # Track nodes in path
        path_len = len(path)
        for i, prefix in enumerate(path):
            # Convert prefix to full hash if known
            full_hash = None
            for neighbor_hash in neighbors:
                if get_hash_prefix(neighbor_hash) == prefix.upper():
                    full_hash = neighbor_hash
                    break
            
            if full_hash:
                stats = node_stats[full_hash]
                path_key = ",".join(path)
                stats["paths"].append(path_key)
                stats["path_timestamps"].append(timestamp)
                stats["positions"].append(path_len - i)  # Position from end
                
                hour_bucket = int(timestamp // 3600)
                stats["active_hours"].add(hour_bucket)
    
    # Calculate mobility metrics for each node
    total_hours = hours
    results = []
    
    for node_hash, stats in node_stats.items():
        if len(stats["paths"]) < min_observations:
            continue
        
        prefix = get_hash_prefix(node_hash)
        
        # Calculate path volatility (how often path changes)
        paths = stats["paths"]
        path_changes = 0
        for i in range(1, len(paths)):
            if paths[i] != paths[i - 1]:
                path_changes += 1
        
        volatility = path_changes / max(1, len(paths) - 1) if len(paths) > 1 else 0
        
        # Calculate active ratio
        active_hours = len(stats["active_hours"])
        active_ratio = active_hours / total_hours if total_hours > 0 else 0
        
        # Calculate average path lifespan
        timestamps = stats["path_timestamps"]
        if len(timestamps) >= 2:
            unique_paths = len(set(stats["paths"]))
            total_span = (max(timestamps) - min(timestamps)) / 3600  # Hours
            avg_lifespan = total_span / max(1, unique_paths - 1) if unique_paths > 1 else total_span
        else:
            avg_lifespan = hours  # Single observation - unknown lifespan
            unique_paths = 1
        
        # Calculate position variance
        positions = stats["positions"]
        if positions:
            mean_pos = sum(positions) / len(positions)
            variance = sum((p - mean_pos) ** 2 for p in positions) / len(positions)
            max_variance = (max(5, max(positions)) ** 2) / 4  # Normalize
            position_variance = min(1.0, variance / max_variance)
        else:
            position_variance = 0
        
        # Classify as mobile
        mobile_score = 0
        if volatility >= MOBILE_VOLATILITY_THRESHOLD:
            mobile_score += 0.4
        if active_ratio <= MOBILE_ACTIVE_RATIO_THRESHOLD:
            mobile_score += 0.2
        if avg_lifespan <= MOBILE_PATH_LIFESPAN_HOURS:
            mobile_score += 0.25
        if position_variance >= MOBILE_POSITION_VARIANCE_THRESHOLD:
            mobile_score += 0.15
        
        is_mobile = mobile_score >= 0.5
        
        # Confidence based on observation count
        confidence = min(1.0, len(paths) / 50)  # Max confidence at 50+ observations
        
        info = MobileNodeInfo(
            hash=node_hash,
            prefix=prefix,
            is_mobile=is_mobile,
            volatility=volatility,
            active_ratio=active_ratio,
            avg_path_lifespan=avg_lifespan,
            position_variance=position_variance,
            confidence=confidence,
            observation_count=len(paths),
            unique_paths=unique_paths,
            active_hours=active_hours,
            total_hours=total_hours,
        )
        results.append(info)
    
    # Sort by mobility score (volatility) descending
    results.sort(key=lambda n: n.volatility, reverse=True)
    
    # Calculate summary stats
    total_nodes = len(results)
    mobile_count = sum(1 for n in results if n.is_mobile)
    fixed_count = total_nodes - mobile_count
    avg_volatility = (
        sum(n.volatility for n in results) / total_nodes
        if total_nodes > 0 else 0
    )
    
    return MobileDetectionStats(
        nodes=results,
        total_nodes=total_nodes,
        mobile_count=mobile_count,
        fixed_count=fixed_count,
        avg_volatility=avg_volatility,
    )


def is_node_mobile(
    conn,
    node_hash: str,
    neighbors: Dict[str, dict],
    hours: int = 24,
) -> Tuple[bool, float]:
    """
    Quick check if a specific node is mobile.
    
    Args:
        conn: SQLite connection
        node_hash: Hash of node to check
        neighbors: Known neighbors
        hours: Time range to analyze
        
    Returns:
        Tuple of (is_mobile, confidence)
    """
    stats = detect_mobile_nodes(conn, neighbors, hours=hours, min_observations=5)
    
    for node in stats.nodes:
        if node.hash == node_hash or node.prefix == get_hash_prefix(node_hash):
            return (node.is_mobile, node.confidence)
    
    return (False, 0.0)  # Not enough data
