"""
Neighbor Affinity - Multi-factor neighbor relationship scoring
==============================================================

Calculates affinity scores between neighbors based on multiple factors:
how consistently they appear in paths together, their geographic proximity,
frequency of co-occurrence, and disambiguation confidence.

This is the backend equivalent of the frontend's buildNeighborAffinity()
function in mesh-topology.ts.

Affinity Factors
----------------
    1. Hop Consistency (30%):
       How consistently does neighbor A appear as a forwarding hop for
       packets from neighbor B? Higher consistency = more reliable link.
       
    2. Geographic Proximity (30%):
       How close are the two neighbors geographically? Closer neighbors
       are more likely to have direct RF links.
       
    3. Frequency (20%):
       How often do packets traverse this neighbor pair? More frequent
       observations increase confidence.
       
    4. Disambiguation Confidence (20%):
       How confident are we in the prefix->hash mapping for both neighbors?
       Low disambiguation confidence reduces affinity certainty.

Use Cases
---------
    - Topology visualization: Weight edges by affinity
    - Route quality analysis: Identify reliable vs flaky links
    - Gateway selection: Find best forwarders to reach a destination
    - Network health: Track affinity changes over time

API Response Format
-------------------
    GET /api/analytics/neighbor_affinity
    
    Returns:
    {
        "success": true,
        "data": {
            "affinities": [
                {
                    "sourceHash": "0xAB123456",
                    "targetHash": "0xCD789ABC",
                    "hopConsistency": 0.85,
                    "geoProximity": 0.90,
                    "frequency": 0.60,
                    "disambiguationConf": 0.95,
                    "combinedScore": 0.82,
                    "observationCount": 150
                }
            ],
            "totalPairs": 45,
            "avgAffinity": 0.72
        }
    }

See Also
--------
    - mesh-topology.ts: Frontend buildNeighborAffinity()
    - disambiguation.py: Provides confidence scores
    - geo_utils.py: Geographic proximity calculations
"""

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from .edge_builder import get_hash_prefix, parse_path
from .geo_utils import (
    calculate_distance,
    get_proximity_score,
    has_valid_coordinates,
)
from .disambiguation import (
    build_prefix_lookup,
    PrefixLookup,
    ResolutionContext,
)

logger = logging.getLogger("Analytics.NeighborAffinity")

# Factor weights (must sum to 1.0)
AFFINITY_WEIGHTS = {
    "hop_consistency": 0.30,
    "geo_proximity": 0.30,
    "frequency": 0.20,
    "disambiguation": 0.20,
}

# Minimum observations to calculate affinity
MIN_OBSERVATIONS = 5


@dataclass
class NeighborAffinity:
    """Affinity metrics between two neighbors."""
    source_hash: str
    target_hash: str
    source_prefix: str
    target_prefix: str
    
    # Factor scores (0-1)
    hop_consistency: float = 0.0
    geo_proximity: float = 0.0
    frequency_score: float = 0.0
    disambiguation_conf: float = 0.0
    
    # Combined
    combined_score: float = 0.0
    
    # Observation data
    observation_count: int = 0
    position_counts: Dict[int, int] = field(default_factory=dict)
    last_seen: float = 0.0
    
    # Geographic data
    distance_meters: Optional[float] = None
    
    def to_dict(self) -> dict:
        return {
            "sourceHash": self.source_hash,
            "targetHash": self.target_hash,
            "sourcePrefix": self.source_prefix,
            "targetPrefix": self.target_prefix,
            "hopConsistency": round(self.hop_consistency, 3),
            "geoProximity": round(self.geo_proximity, 3),
            "frequency": round(self.frequency_score, 3),
            "disambiguationConf": round(self.disambiguation_conf, 3),
            "combinedScore": round(self.combined_score, 3),
            "observationCount": self.observation_count,
            "distanceMeters": round(self.distance_meters, 1) if self.distance_meters else None,
            "lastSeen": self.last_seen,
        }


@dataclass
class AffinityStats:
    """Statistics about neighbor affinity analysis."""
    affinities: List[NeighborAffinity]
    total_pairs: int = 0
    avg_affinity: float = 0.0
    high_affinity_pairs: int = 0  # > 0.7
    low_affinity_pairs: int = 0   # < 0.3
    
    def to_dict(self) -> dict:
        return {
            "affinities": [a.to_dict() for a in self.affinities],
            "totalPairs": self.total_pairs,
            "avgAffinity": round(self.avg_affinity, 3),
            "highAffinityPairs": self.high_affinity_pairs,
            "lowAffinityPairs": self.low_affinity_pairs,
        }


def get_neighbor_affinity(
    conn,
    neighbors: Dict[str, dict],
    local_hash: Optional[str] = None,
    local_lat: Optional[float] = None,
    local_lon: Optional[float] = None,
    hours: int = 168,
    min_observations: int = MIN_OBSERVATIONS,
) -> AffinityStats:
    """
    Calculate affinity scores between all neighbor pairs.
    
    Args:
        conn: SQLite connection
        neighbors: Dict of known neighbors with location data
        local_hash: Local node's hash
        local_lat: Local node's latitude
        local_lon: Local node's longitude
        hours: Time range to analyze
        min_observations: Minimum observations to include pair
        
    Returns:
        AffinityStats with all neighbor pair affinities
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
    
    # Track consecutive prefix pairs
    pair_stats: Dict[Tuple[str, str], NeighborAffinity] = {}
    max_observations = 1
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT timestamp, original_path, forwarded_path
        FROM packets
        WHERE timestamp > ?
          AND (original_path IS NOT NULL OR forwarded_path IS NOT NULL)
        ORDER BY timestamp
    """, (cutoff,))
    
    for row in cursor.fetchall():
        timestamp = row[0]
        raw_path = row[2] or row[1]
        
        path = parse_path(raw_path)
        if not path or len(path) < 2:
            continue
        
        # Process consecutive pairs
        for i in range(len(path) - 1):
            from_prefix = path[i].upper()
            to_prefix = path[i + 1].upper()
            
            if not from_prefix or not to_prefix or from_prefix == to_prefix:
                continue
            
            # Create canonical pair key (sorted)
            if from_prefix < to_prefix:
                pair_key = (from_prefix, to_prefix)
            else:
                pair_key = (to_prefix, from_prefix)
            
            # Get or create stats
            if pair_key not in pair_stats:
                # Resolve prefixes to hashes
                from_hash, from_conf = lookup.resolve(from_prefix)
                to_hash, to_conf = lookup.resolve(to_prefix)
                
                pair_stats[pair_key] = NeighborAffinity(
                    source_hash=from_hash or f"0x{from_prefix}",
                    target_hash=to_hash or f"0x{to_prefix}",
                    source_prefix=from_prefix,
                    target_prefix=to_prefix,
                )
            
            stats = pair_stats[pair_key]
            stats.observation_count += 1
            stats.last_seen = max(stats.last_seen, timestamp)
            
            # Track position
            position = len(path) - i
            stats.position_counts[position] = stats.position_counts.get(position, 0) + 1
            
            max_observations = max(max_observations, stats.observation_count)
    
    # Calculate scores for each pair
    result_affinities = []
    
    for pair_key, stats in pair_stats.items():
        if stats.observation_count < min_observations:
            continue
        
        # 1. Hop consistency: how concentrated are observations at specific positions?
        if stats.position_counts:
            total_obs = sum(stats.position_counts.values())
            max_pos_count = max(stats.position_counts.values())
            stats.hop_consistency = max_pos_count / total_obs if total_obs > 0 else 0
        
        # 2. Geographic proximity
        from_neighbor = neighbors.get(stats.source_hash, {})
        to_neighbor = neighbors.get(stats.target_hash, {})
        
        from_lat = from_neighbor.get("latitude")
        from_lon = from_neighbor.get("longitude")
        to_lat = to_neighbor.get("latitude")
        to_lon = to_neighbor.get("longitude")
        
        if has_valid_coordinates(from_lat, from_lon) and has_valid_coordinates(to_lat, to_lon):
            distance = calculate_distance(from_lat, from_lon, to_lat, to_lon)
            stats.distance_meters = distance
            stats.geo_proximity = get_proximity_score(distance)
        else:
            stats.geo_proximity = 0.5  # Unknown location - neutral
        
        # 3. Frequency score (normalized)
        stats.frequency_score = stats.observation_count / max_observations
        
        # 4. Disambiguation confidence (average of both)
        _, from_conf = lookup.resolve(stats.source_prefix)
        _, to_conf = lookup.resolve(stats.target_prefix)
        stats.disambiguation_conf = (from_conf + to_conf) / 2
        
        # Combined score
        stats.combined_score = (
            stats.hop_consistency * AFFINITY_WEIGHTS["hop_consistency"] +
            stats.geo_proximity * AFFINITY_WEIGHTS["geo_proximity"] +
            stats.frequency_score * AFFINITY_WEIGHTS["frequency"] +
            stats.disambiguation_conf * AFFINITY_WEIGHTS["disambiguation"]
        )
        
        result_affinities.append(stats)
    
    # Sort by combined score descending
    result_affinities.sort(key=lambda a: a.combined_score, reverse=True)
    
    # Calculate statistics
    total_pairs = len(result_affinities)
    avg_affinity = (
        sum(a.combined_score for a in result_affinities) / total_pairs
        if total_pairs > 0 else 0
    )
    high_affinity = sum(1 for a in result_affinities if a.combined_score > 0.7)
    low_affinity = sum(1 for a in result_affinities if a.combined_score < 0.3)
    
    return AffinityStats(
        affinities=result_affinities,
        total_pairs=total_pairs,
        avg_affinity=avg_affinity,
        high_affinity_pairs=high_affinity,
        low_affinity_pairs=low_affinity,
    )


def get_affinity_for_node(
    conn,
    node_hash: str,
    neighbors: Dict[str, dict],
    local_hash: Optional[str] = None,
    local_lat: Optional[float] = None,
    local_lon: Optional[float] = None,
    hours: int = 168,
) -> List[NeighborAffinity]:
    """
    Get affinity scores for a specific node to all its neighbors.
    
    Args:
        conn: SQLite connection
        node_hash: Hash of node to analyze
        neighbors: Dict of known neighbors
        local_hash: Local node's hash
        local_lat: Local latitude
        local_lon: Local longitude
        hours: Time range to analyze
        
    Returns:
        List of NeighborAffinity for all nodes connected to target
    """
    all_affinities = get_neighbor_affinity(
        conn,
        neighbors,
        local_hash,
        local_lat,
        local_lon,
        hours,
    )
    
    # Filter to affinities involving the target node
    node_prefix = get_hash_prefix(node_hash)
    
    return [
        a for a in all_affinities.affinities
        if a.source_hash == node_hash or a.target_hash == node_hash
        or a.source_prefix == node_prefix or a.target_prefix == node_prefix
    ]
