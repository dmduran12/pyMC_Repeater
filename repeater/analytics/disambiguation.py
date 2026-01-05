"""
Disambiguation - Prefix collision resolution (Enhanced 4-Factor System)
=======================================================================

Resolves 2-character hash prefix collisions using a sophisticated 4-factor
weighted scoring system. This matches the frontend's prefix-disambiguation.ts
implementation for consistent results between backend and frontend.

The Problem
-----------
MeshCore uses 2-character prefixes for compact path representation.
With 180+ nodes and only 256 possible prefixes, collisions are inevitable:

    - Node A: 0xABCDEF12 -> prefix "AB"
    - Node B: 0xAB998877 -> prefix "AB"
    
When we see "AB" in a packet path, which node is it?

4-Factor Scoring System
-----------------------
The system uses four weighted factors to score candidates:

    1. Position Consistency (15%):
       How consistently does this candidate appear at specific hop positions?
       Shared across all candidates matching the same prefix.
       
    2. Co-occurrence Frequency (15%):
       How often does this prefix appear alongside specific other prefixes?
       Shared across all candidates matching the same prefix.
       
    3. Geographic Scoring (35%):
       Distance-based scoring with multiple evidence sources:
       - Distance to local node (closer = higher score)
       - Source-geographic correlation (position-1 proximity to packet source)
       - Previous-hop anchor (proximity to resolved upstream node)
       - Next-hop anchor (proximity to resolved downstream node)
       - Zero-hop boost for known direct RF contacts
       
    4. Recency Scoring (35%):
       When was this node last seen? Uses exponential decay:
       score = e^(-hours/12)
       Nodes not seen in 14 days are filtered out entirely.

Additional Confidence Boosts
----------------------------
    - Dominant forwarder boost: 80%+ position-1 appearances -> +0.3 to +0.6
    - Score-weighted redistribution: Reallocates shared counts by combined score
    - Source-geographic evidence boost: 50%+ more geo evidence -> up to +0.3

Constants
---------
    SCORE_WEIGHTS:
        position: 0.15
        cooccurrence: 0.15
        geographic: 0.35
        recency: 0.35
        
    MAX_CANDIDATE_AGE_HOURS = 336 (14 days)
    RECENCY_DECAY_HOURS = 12
    MAX_POSITIONS = 5

Public Functions
----------------
    build_prefix_lookup(conn, neighbors, local_hash, local_lat, local_lon, hours)
        Build comprehensive lookup table from database.
        
    resolve_prefix(lookup, prefix, context)
        Resolve prefix to hash with context-aware scoring.
        
    get_disambiguation_stats(conn, neighbors, local_hash)
        Get statistics for API response.

Public Classes
--------------
    PrefixLookup:
        Complete disambiguation engine with all scoring factors.
        
    DisambiguationCandidate:
        Candidate with all scoring components.
        
    DisambiguationResult:
        Resolution result with confidence metrics.
        
    DisambiguationStats:
        Overall collision statistics.

See Also
--------
    - pymc_console/frontend/src/lib/prefix-disambiguation.ts: Frontend equivalent
    - pymc_console/frontend/src/lib/geo-utils.ts: Frontend geographic utilities
    - edge_builder.py: Uses disambiguation for edge identification
"""

import json
import logging
import math
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from .edge_builder import get_hash_prefix, parse_path
from .geo_utils import (
    calculate_distance,
    get_proximity_score,
    has_valid_coordinates,
    get_anchor_distance_evidence,
    PROXIMITY_BANDS,
)

logger = logging.getLogger("Analytics.Disambiguation")

# ═══════════════════════════════════════════════════════════════════════════════
# Constants (matching frontend)
# ═══════════════════════════════════════════════════════════════════════════════

# Score weights (must sum to 1.0)
SCORE_WEIGHTS = {
    "position": 0.15,      # Shared across collision candidates
    "cooccurrence": 0.15,  # Shared across collision candidates
    "geographic": 0.35,    # Candidate-specific
    "recency": 0.35,       # Candidate-specific
}

# Maximum hop positions to track
MAX_POSITIONS = 5

# Maximum age for candidates (hours) - matches frontend MAX_CANDIDATE_AGE_HOURS
MAX_CANDIDATE_AGE_HOURS = 336  # 14 days

# Recency decay half-life (hours) - matches frontend RECENCY_DECAY_HOURS
RECENCY_DECAY_HOURS = 12

# Confidence thresholds
HIGH_CONFIDENCE = 0.9
MEDIUM_CONFIDENCE = 0.6
LOW_CONFIDENCE = 0.4


# ═══════════════════════════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_recency_score(last_seen_timestamp: float, now: Optional[float] = None) -> float:
    """
    Calculate recency score using exponential decay.
    
    Matches frontend: score = e^(-hours/12)
    
    Args:
        last_seen_timestamp: Unix timestamp when node was last seen
        now: Current timestamp (defaults to time.time())
        
    Returns:
        Score from 0.0 to 1.0
    """
    if not last_seen_timestamp or last_seen_timestamp <= 0:
        return 0.1  # Unknown recency gets low score
    
    now = now or time.time()
    hours_ago = (now - last_seen_timestamp) / 3600
    
    if hours_ago < 0:
        return 1.0  # Future timestamp (clock skew) - assume recent
    
    # Exponential decay: e^(-hours/12)
    return math.exp(-hours_ago / RECENCY_DECAY_HOURS)


def is_candidate_too_old(last_seen_timestamp: float, now: Optional[float] = None) -> bool:
    """
    Check if a candidate is too old to be considered.
    
    Args:
        last_seen_timestamp: Unix timestamp when node was last seen
        now: Current timestamp (defaults to time.time())
        
    Returns:
        True if candidate should be filtered out
    """
    if not last_seen_timestamp or last_seen_timestamp <= 0:
        return False  # Unknown age - don't filter (could be local node)
    
    now = now or time.time()
    hours_ago = (now - last_seen_timestamp) / 3600
    
    return hours_ago > MAX_CANDIDATE_AGE_HOURS


def is_repeater(neighbor: dict) -> bool:
    """
    Determine if a neighbor is a repeater (participates in mesh routing).
    
    Only repeaters should be included in prefix disambiguation because
    companions don't forward packets, so their prefixes never appear in paths.
    
    Args:
        neighbor: Neighbor info dict
        
    Returns:
        True if this is a repeater
    """
    contact_type = neighbor.get("contact_type", "")
    if contact_type:
        ct = contact_type.lower()
        if ct in ("repeater", "rep"):
            return True
        if ct in ("companion", "client", "cli", "room server", "room_server", "room", "server"):
            return False
    
    # Fallback to is_repeater flag
    is_rep = neighbor.get("is_repeater")
    if is_rep is True:
        return True
    if is_rep is False:
        return False
    
    # Unknown type - be conservative and exclude
    return False


def filter_repeaters_only(neighbors: Dict[str, dict]) -> Dict[str, dict]:
    """Filter neighbors to only include repeaters."""
    return {h: n for h, n in neighbors.items() if is_repeater(n)}


def get_position_from_index(index: int, path_length: int) -> int:
    """
    Convert path array index to position number.
    
    Position 1 = last hop (direct forwarder to local)
    Position 2 = second-to-last, etc.
    
    Args:
        index: 0-based index in path array
        path_length: Total length of path
        
    Returns:
        Position number (1 = closest to local)
    """
    return path_length - index


# ═══════════════════════════════════════════════════════════════════════════════
# Data Classes
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class DisambiguationCandidate:
    """Statistics for a single candidate matching a prefix."""
    hash: str
    prefix: str
    
    # Position scoring
    position_counts: List[int] = field(default_factory=lambda: [0] * MAX_POSITIONS)
    total_appearances: int = 0
    typical_position: int = 0
    position_consistency: float = 0.0
    
    # Co-occurrence scoring
    adjacent_prefix_counts: Dict[str, int] = field(default_factory=dict)
    total_adjacent_observations: int = 0
    
    # Geographic data
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    distance_to_local: Optional[float] = None
    is_zero_hop: bool = False
    
    # Source-geographic evidence
    src_geo_evidence_score: float = 0.0
    src_geo_evidence_count: int = 0
    
    # Recency
    last_seen_timestamp: float = 0.0
    recency_score: float = 0.1
    
    # Combined scores
    position_score: float = 0.0
    cooccurrence_score: float = 0.0
    geographic_score: float = 0.2  # Default for unknown location
    combined_score: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            "hash": self.hash,
            "prefix": self.prefix,
            "positionCounts": self.position_counts,
            "totalAppearances": self.total_appearances,
            "typicalPosition": self.typical_position,
            "positionConsistency": round(self.position_consistency, 3),
            "geographicScore": round(self.geographic_score, 3),
            "recencyScore": round(self.recency_score, 3),
            "combinedScore": round(self.combined_score, 3),
            "distanceToLocal": self.distance_to_local,
            "isZeroHop": self.is_zero_hop,
            "lastSeenTimestamp": self.last_seen_timestamp,
        }


@dataclass
class DisambiguationResult:
    """Result of disambiguating a single prefix."""
    prefix: str
    candidates: List[DisambiguationCandidate]
    best_match: Optional[str]
    confidence: float
    is_unambiguous: bool
    best_match_for_position: Dict[int, Tuple[str, float]] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            "prefix": self.prefix,
            "candidates": [c.to_dict() for c in self.candidates],
            "bestMatch": self.best_match,
            "confidence": round(self.confidence, 3),
            "isUnambiguous": self.is_unambiguous,
            "bestMatchForPosition": {
                str(k): {"hash": v[0], "confidence": round(v[1], 3)}
                for k, v in self.best_match_for_position.items()
            },
        }


@dataclass
class DisambiguationStats:
    """Overall disambiguation statistics."""
    total_prefixes: int = 0
    unambiguous_prefixes: int = 0
    collision_prefixes: int = 0
    collision_rate: float = 0.0
    avg_confidence: float = 0.0
    low_confidence_prefixes: List[str] = field(default_factory=list)
    high_collision_prefixes: List[dict] = field(default_factory=list)
    total_candidates: int = 0
    repeaters_only: bool = True
    
    def to_dict(self) -> dict:
        return {
            "totalPrefixes": self.total_prefixes,
            "unambiguousPrefixes": self.unambiguous_prefixes,
            "collisionPrefixes": self.collision_prefixes,
            "collisionRate": round(self.collision_rate, 1),
            "avgConfidence": round(self.avg_confidence, 3),
            "lowConfidencePrefixes": self.low_confidence_prefixes,
            "highCollisionPrefixes": self.high_collision_prefixes,
            "totalCandidates": self.total_candidates,
            "repeatersOnly": self.repeaters_only,
        }


@dataclass
class ResolutionContext:
    """Context for position-aware resolution."""
    position: Optional[int] = None
    adjacent_prefixes: List[str] = field(default_factory=list)
    is_last_hop: bool = False


# ═══════════════════════════════════════════════════════════════════════════════
# PrefixLookup Class
# ═══════════════════════════════════════════════════════════════════════════════

class PrefixLookup:
    """
    Prefix disambiguation lookup table.
    
    Maintains candidate hashes for each 2-char prefix and computes
    confidence scores using the 4-factor weighted scoring system.
    """
    
    def __init__(self):
        self.results: Dict[str, DisambiguationResult] = {}
        self.prefix_to_candidates: Dict[str, List[DisambiguationCandidate]] = defaultdict(list)
        self.local_hash: Optional[str] = None
        self.local_lat: Optional[float] = None
        self.local_lon: Optional[float] = None
    
    def get(self, prefix: str) -> Optional[DisambiguationResult]:
        """Get disambiguation result for a prefix."""
        return self.results.get(prefix.upper())
    
    def resolve(
        self,
        prefix: str,
        context: Optional[ResolutionContext] = None,
    ) -> Tuple[Optional[str], float]:
        """
        Resolve a prefix to a hash using the lookup table.
        
        Args:
            prefix: 2-char prefix to resolve
            context: Optional context for position-aware resolution
            
        Returns:
            Tuple of (best hash, confidence)
        """
        normalized = prefix.upper()
        result = self.results.get(normalized)
        
        if not result or not result.candidates:
            return (None, 0.0)
        
        context = context or ResolutionContext()
        
        # For last hop (direct forwarder), use global confidence with boosts
        if context.is_last_hop or context.position == 1:
            return (result.best_match, result.confidence)
        
        # If position context provided and we have position-specific data
        if context.position and context.position in result.best_match_for_position:
            pos_match = result.best_match_for_position[context.position]
            # Use max of position confidence and global confidence to preserve boosts
            best_conf = max(pos_match[1], result.confidence)
            return (pos_match[0], best_conf)
        
        # If adjacent prefixes provided, boost candidates with high co-occurrence
        if context.adjacent_prefixes:
            best_hash = result.best_match
            best_score = 0.0
            
            for candidate in result.candidates:
                co_score = sum(
                    candidate.adjacent_prefix_counts.get(adj.upper(), 0)
                    for adj in context.adjacent_prefixes
                )
                total_adj = max(1, candidate.total_adjacent_observations)
                total_score = candidate.combined_score + (co_score / total_adj) * 0.3
                
                if total_score > best_score:
                    best_score = total_score
                    best_hash = candidate.hash
            
            return (best_hash, result.confidence)
        
        # Default: return global best match
        return (result.best_match, result.confidence)
    
    def get_stats(self) -> DisambiguationStats:
        """Get overall disambiguation statistics."""
        total = len(self.results)
        unambiguous = sum(1 for r in self.results.values() if r.is_unambiguous)
        collisions = total - unambiguous
        
        confidences = [r.confidence for r in self.results.values()]
        avg_conf = sum(confidences) / len(confidences) if confidences else 0
        
        low_conf = [
            r.prefix for r in self.results.values()
            if r.confidence < 0.5 and not r.is_unambiguous
        ]
        
        high_collision = [
            {
                "prefix": r.prefix,
                "candidateCount": len(r.candidates),
                "candidateHashes": [c.hash for c in r.candidates],
            }
            for r in self.results.values()
            if len(r.candidates) >= 3
        ]
        high_collision.sort(key=lambda x: x["candidateCount"], reverse=True)
        
        total_candidates = sum(len(r.candidates) for r in self.results.values())
        
        return DisambiguationStats(
            total_prefixes=total,
            unambiguous_prefixes=unambiguous,
            collision_prefixes=collisions,
            collision_rate=(collisions / total * 100) if total > 0 else 0,
            avg_confidence=avg_conf,
            low_confidence_prefixes=low_conf[:10],
            high_collision_prefixes=high_collision[:5],
            total_candidates=total_candidates,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Main Build Function
# ═══════════════════════════════════════════════════════════════════════════════

def build_prefix_lookup(
    conn,
    neighbors: Dict[str, dict],
    local_hash: Optional[str] = None,
    local_lat: Optional[float] = None,
    local_lon: Optional[float] = None,
    hours: int = 168,
) -> PrefixLookup:
    """
    Build prefix lookup table from database packets.
    
    This is the main entry point for disambiguation, matching the frontend's
    buildPrefixLookup() function.
    
    Args:
        conn: SQLite connection
        neighbors: Dict of known neighbors
        local_hash: Local node's hash
        local_lat: Local node's latitude
        local_lon: Local node's longitude
        hours: Time range to analyze (default: 168 = 7 days)
        
    Returns:
        Populated PrefixLookup with all scoring computed
    """
    lookup = PrefixLookup()
    lookup.local_hash = local_hash
    lookup.local_lat = local_lat
    lookup.local_lon = local_lon
    
    has_local_coords = has_valid_coordinates(local_lat, local_lon)
    now = time.time()
    
    # ─── Step 0: Filter to repeaters only ────────────────────────────────────────
    repeaters_only = filter_repeaters_only(neighbors)
    
    # ─── Step 1: Build prefix -> candidates mapping ──────────────────────────────
    prefix_to_candidates: Dict[str, List[DisambiguationCandidate]] = defaultdict(list)
    
    # Add local node if hash provided
    if local_hash:
        local_prefix = get_hash_prefix(local_hash)
        candidate = DisambiguationCandidate(
            hash=local_hash.upper(),
            prefix=local_prefix,
            latitude=local_lat,
            longitude=local_lon,
            distance_to_local=0,
            last_seen_timestamp=now,  # Local is always "just seen"
            recency_score=1.0,
            geographic_score=1.0,  # Local is always at distance 0
        )
        prefix_to_candidates[local_prefix].append(candidate)
    
    # Add all repeater neighbors (with age filtering)
    for neighbor_hash, neighbor in repeaters_only.items():
        prefix = get_hash_prefix(neighbor_hash)
        last_seen = neighbor.get("last_seen", 0)
        
        # Skip candidates that are too old
        if is_candidate_too_old(last_seen, now):
            continue
        
        # Calculate distance to local
        distance_to_local = None
        neighbor_lat = neighbor.get("latitude")
        neighbor_lon = neighbor.get("longitude")
        is_zero_hop = neighbor.get("zero_hop", False)
        
        if has_local_coords and has_valid_coordinates(neighbor_lat, neighbor_lon):
            distance_to_local = calculate_distance(
                local_lat, local_lon,
                neighbor_lat, neighbor_lon
            )
        
        # Calculate initial geographic score based on distance
        geo_score = 0.2  # Default for unknown location
        if distance_to_local is not None:
            geo_score = get_proximity_score(distance_to_local)
        elif has_valid_coordinates(neighbor_lat, neighbor_lon):
            geo_score = 0.5  # Has coords but no local coords - neutral
        
        # Zero-hop boost: known direct RF contact
        if is_zero_hop:
            geo_score = max(geo_score, 0.95)
        
        # Calculate recency score
        recency_score = calculate_recency_score(last_seen, now)
        
        candidate = DisambiguationCandidate(
            hash=neighbor_hash.upper(),
            prefix=prefix,
            latitude=neighbor_lat,
            longitude=neighbor_lon,
            distance_to_local=distance_to_local,
            is_zero_hop=is_zero_hop,
            last_seen_timestamp=last_seen,
            recency_score=recency_score,
            geographic_score=geo_score,
        )
        prefix_to_candidates[prefix].append(candidate)
    
    lookup.prefix_to_candidates = prefix_to_candidates
    
    # Create hash -> candidate lookup for anchor correlation
    hash_to_candidate: Dict[str, DisambiguationCandidate] = {}
    for candidates in prefix_to_candidates.values():
        for c in candidates:
            hash_to_candidate[c.hash] = c
    
    # ─── Step 2: Analyze packets for position and co-occurrence data ─────────────
    cutoff = now - (hours * 3600)
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
        raw_path = row[3] or row[2]  # Prefer forwarded_path
        
        path = parse_path(raw_path)
        if not path:
            continue
        
        path_length = len(path)
        
        # Get source node info for geographic correlation
        src_neighbor = neighbors.get(src_hash) if src_hash else None
        src_has_coords = (
            src_neighbor and
            has_valid_coordinates(
                src_neighbor.get("latitude"),
                src_neighbor.get("longitude")
            )
        )
        
        # Process each element in the path
        for i, prefix in enumerate(path):
            prefix = prefix.upper()
            candidates = prefix_to_candidates.get(prefix)
            if not candidates:
                continue
            
            # Position: 1 = last element (direct forwarder), 2 = second-to-last, etc.
            position = get_position_from_index(i, path_length)
            position_index = min(position - 1, MAX_POSITIONS - 1)
            
            # Update position counts for all candidates matching this prefix
            for candidate in candidates:
                candidate.position_counts[position_index] += 1
                candidate.total_appearances += 1
                
                # === SOURCE-GEOGRAPHIC CORRELATION ===
                # For position 1 (last hop) with multiple candidates
                if (position == 1 and len(candidates) > 1 and src_has_coords and
                        has_valid_coordinates(candidate.latitude, candidate.longitude)):
                    evidence = get_anchor_distance_evidence(
                        candidate.latitude, candidate.longitude,
                        src_neighbor["latitude"], src_neighbor["longitude"]
                    )
                    
                    # Also factor in distance to local
                    if candidate.distance_to_local is not None:
                        if candidate.distance_to_local < 500:
                            evidence *= 1.2
                        elif candidate.distance_to_local < 2000:
                            evidence *= 1.0
                        else:
                            evidence *= 0.8
                    
                    candidate.src_geo_evidence_score += evidence
                    candidate.src_geo_evidence_count += 1
                
                # === PREVIOUS-HOP ANCHOR CORRELATION ===
                if (i > 0 and len(candidates) > 1 and
                        has_valid_coordinates(candidate.latitude, candidate.longitude)):
                    prev_hop_prefix = path[i - 1].upper()
                    prev_hop_candidates = prefix_to_candidates.get(prev_hop_prefix, [])
                    
                    if prev_hop_candidates:
                        anchor_lat, anchor_lon, anchor_conf = _get_best_anchor(prev_hop_candidates)
                        
                        if anchor_lat is not None and anchor_conf > 0.4:
                            evidence = get_anchor_distance_evidence(
                                candidate.latitude, candidate.longitude,
                                anchor_lat, anchor_lon
                            )
                            evidence *= anchor_conf
                            candidate.src_geo_evidence_score += evidence
                            candidate.src_geo_evidence_count += 1
                
                # === NEXT-HOP ANCHOR CORRELATION ===
                if (position > 1 and len(candidates) > 1 and i + 1 < path_length and
                        has_valid_coordinates(candidate.latitude, candidate.longitude)):
                    next_hop_prefix = path[i + 1].upper()
                    next_hop_candidates = prefix_to_candidates.get(next_hop_prefix, [])
                    
                    if next_hop_candidates:
                        anchor_lat, anchor_lon, anchor_conf = _get_best_anchor(next_hop_candidates)
                        
                        if anchor_lat is not None and anchor_conf > 0.4:
                            evidence = get_anchor_distance_evidence(
                                candidate.latitude, candidate.longitude,
                                anchor_lat, anchor_lon
                            )
                            evidence *= anchor_conf
                            candidate.src_geo_evidence_score += evidence
                            candidate.src_geo_evidence_count += 1
                
                # Track adjacent prefixes
                if i > 0:
                    prev_prefix = path[i - 1].upper()
                    candidate.adjacent_prefix_counts[prev_prefix] = \
                        candidate.adjacent_prefix_counts.get(prev_prefix, 0) + 1
                    candidate.total_adjacent_observations += 1
                    
                if i < path_length - 1:
                    next_prefix = path[i + 1].upper()
                    candidate.adjacent_prefix_counts[next_prefix] = \
                        candidate.adjacent_prefix_counts.get(next_prefix, 0) + 1
                    candidate.total_adjacent_observations += 1
    
    # ─── Step 3: Calculate scores for each candidate ─────────────────────────────
    # Find max values for normalization
    max_appearances = 1
    max_adjacent_obs = 1
    
    for candidates in prefix_to_candidates.values():
        for c in candidates:
            max_appearances = max(max_appearances, c.total_appearances)
            max_adjacent_obs = max(max_adjacent_obs, c.total_adjacent_observations)
    
    # Calculate scores
    for candidates in prefix_to_candidates.values():
        for candidate in candidates:
            # Position score
            if candidate.total_appearances > 0:
                # Find typical position (mode)
                max_count = 0
                typical_pos = 1
                for i in range(MAX_POSITIONS):
                    if candidate.position_counts[i] > max_count:
                        max_count = candidate.position_counts[i]
                        typical_pos = i + 1
                
                candidate.typical_position = typical_pos
                candidate.position_consistency = max_count / candidate.total_appearances
                
                # Position score = consistency * 0.6 + frequency * 0.4
                frequency_score = candidate.total_appearances / max_appearances
                candidate.position_score = (
                    candidate.position_consistency * 0.6 +
                    frequency_score * 0.4
                )
            
            # Co-occurrence score
            if candidate.total_adjacent_observations > 0:
                candidate.cooccurrence_score = (
                    candidate.total_adjacent_observations / max_adjacent_obs
                )
            
            # Combined score (4-factor calculation)
            candidate.combined_score = (
                candidate.position_score * SCORE_WEIGHTS["position"] +
                candidate.cooccurrence_score * SCORE_WEIGHTS["cooccurrence"] +
                candidate.geographic_score * SCORE_WEIGHTS["geographic"] +
                candidate.recency_score * SCORE_WEIGHTS["recency"]
            )
            
            # Source-geographic evidence boost
            if candidate.src_geo_evidence_count > 0:
                avg_evidence = candidate.src_geo_evidence_score / candidate.src_geo_evidence_count
                observation_weight = min(candidate.src_geo_evidence_count / 50, 1)
                src_geo_boost = avg_evidence * observation_weight * 0.3
                candidate.combined_score += src_geo_boost
    
    # ─── Step 4: Build disambiguation results ────────────────────────────────────
    for prefix, candidates in prefix_to_candidates.items():
        # Sort by combined score descending
        candidates.sort(key=lambda c: c.combined_score, reverse=True)
        
        best_match = candidates[0].hash if candidates else None
        
        # Calculate confidence based on score separation
        confidence = 0.0
        if len(candidates) == 1:
            confidence = 1.0  # Only one candidate = 100% confident
        elif len(candidates) > 1:
            best = candidates[0].combined_score
            second = candidates[1].combined_score
            
            if best > 0:
                confidence = min(1.0, (best - second) / best)
            
            # Boost confidence if best has significantly more appearances
            if candidates[0].total_appearances > candidates[1].total_appearances * 2:
                confidence = min(1.0, confidence + 0.2)
            
            # === DOMINANT FORWARDER BOOST ===
            pos1_index = 0
            best_pos1 = candidates[0].position_counts[pos1_index]
            second_pos1 = candidates[1].position_counts[pos1_index]
            total_pos1 = best_pos1 + second_pos1
            
            if total_pos1 >= 20 and best_pos1 >= 10:
                pos1_ratio = best_pos1 / total_pos1
                if pos1_ratio >= 0.80:
                    # Scale boost: 80% = +0.3, 90% = +0.45, 100% = +0.6
                    dominance_boost = 0.30 + (pos1_ratio - 0.80) * 1.5
                    confidence = min(1.0, confidence + dominance_boost)
            
            # === SCORE-WEIGHTED REDISTRIBUTION ===
            total_score = sum(c.combined_score for c in candidates)
            if total_score > 0:
                raw_pos1_total = sum(c.position_counts[0] for c in candidates)
                
                weighted_pos1_counts = []
                for c in candidates:
                    weight = c.combined_score / total_score
                    weighted_pos1 = raw_pos1_total * weight
                    weighted_pos1_counts.append(weighted_pos1)
                
                best_weighted_pos1 = weighted_pos1_counts[0]
                second_weighted_pos1 = weighted_pos1_counts[1] if len(weighted_pos1_counts) > 1 else 0
                weighted_total = best_weighted_pos1 + second_weighted_pos1
                
                if weighted_total >= 20 and best_weighted_pos1 >= 10:
                    weighted_ratio = best_weighted_pos1 / weighted_total
                    if weighted_ratio >= 0.60:
                        # Scale: 60% = +0.2, 80% = +0.4, 100% = +0.6
                        dominance_boost = 0.20 + (weighted_ratio - 0.60) * 1.0
                        confidence = min(1.0, confidence + dominance_boost)
            
            # === SOURCE-GEOGRAPHIC EVIDENCE BOOST ===
            best_geo_evidence = candidates[0].src_geo_evidence_score
            second_geo_evidence = candidates[1].src_geo_evidence_score
            best_geo_count = candidates[0].src_geo_evidence_count
            
            if best_geo_count >= 10 and best_geo_evidence > second_geo_evidence * 1.5:
                evidence_ratio = (
                    best_geo_evidence / (best_geo_evidence + second_geo_evidence)
                    if second_geo_evidence > 0 else 1.0
                )
                src_geo_conf_boost = min(0.3, (evidence_ratio - 0.5) * 0.6)
                confidence = min(1.0, confidence + src_geo_conf_boost)
        
        # Build position-specific best matches
        best_match_for_position: Dict[int, Tuple[str, float]] = {}
        for pos in range(1, MAX_POSITIONS + 1):
            sorted_by_pos = sorted(
                candidates,
                key=lambda c: c.position_counts[pos - 1] if pos <= len(c.position_counts) else 0,
                reverse=True
            )
            
            if sorted_by_pos and sorted_by_pos[0].position_counts[pos - 1] > 0:
                best_for_pos = sorted_by_pos[0]
                pos_confidence = 1.0
                
                if len(sorted_by_pos) > 1:
                    best_count = best_for_pos.position_counts[pos - 1]
                    second_count = sorted_by_pos[1].position_counts[pos - 1]
                    total = best_count + second_count
                    pos_confidence = best_count / total if total > 0 else 0
                
                best_match_for_position[pos] = (best_for_pos.hash, pos_confidence)
        
        result = DisambiguationResult(
            prefix=prefix,
            candidates=candidates,
            best_match=best_match,
            confidence=confidence,
            is_unambiguous=len(candidates) == 1,
            best_match_for_position=best_match_for_position,
        )
        
        lookup.results[prefix] = result
    
    return lookup


def _get_best_anchor(candidates: List[DisambiguationCandidate]) -> Tuple[Optional[float], Optional[float], float]:
    """
    Get the best anchor location from a list of candidates.
    
    Returns:
        Tuple of (latitude, longitude, confidence)
    """
    if len(candidates) == 1:
        c = candidates[0]
        if has_valid_coordinates(c.latitude, c.longitude):
            return (c.latitude, c.longitude, 1.0)
        return (None, None, 0.0)
    
    # Use best-scoring candidate with coords
    sorted_candidates = sorted(candidates, key=lambda c: c.combined_score, reverse=True)
    best = sorted_candidates[0]
    second = sorted_candidates[1] if len(sorted_candidates) > 1 else None
    
    if has_valid_coordinates(best.latitude, best.longitude) and best.combined_score > 0:
        score_separation = (
            (best.combined_score - second.combined_score) / best.combined_score
            if second else 1.0
        )
        anchor_conf = min(1.0, score_separation + 0.3)
        return (best.latitude, best.longitude, anchor_conf)
    
    return (None, None, 0.0)


# ═══════════════════════════════════════════════════════════════════════════════
# Public Interface
# ═══════════════════════════════════════════════════════════════════════════════

def resolve_prefix(
    lookup: PrefixLookup,
    prefix: str,
    context: Optional[ResolutionContext] = None,
) -> Tuple[Optional[str], float]:
    """
    Resolve a prefix to a hash using the lookup table.
    
    Wrapper around PrefixLookup.resolve() for convenient API.
    
    Args:
        lookup: PrefixLookup instance
        prefix: 2-char prefix to resolve
        context: Optional context for position-aware resolution
        
    Returns:
        Tuple of (best hash, confidence)
    """
    return lookup.resolve(prefix, context)


def get_disambiguation_stats(
    conn,
    neighbors: dict,
    local_hash: Optional[str] = None,
    local_lat: Optional[float] = None,
    local_lon: Optional[float] = None,
) -> DisambiguationStats:
    """
    Get disambiguation statistics for API response.
    
    Args:
        conn: SQLite connection
        neighbors: Dict of known neighbors
        local_hash: Local node's hash
        local_lat: Local node's latitude (optional)
        local_lon: Local node's longitude (optional)
        
    Returns:
        DisambiguationStats
    """
    lookup = build_prefix_lookup(
        conn,
        neighbors,
        local_hash,
        local_lat,
        local_lon,
    )
    return lookup.get_stats()


def get_candidates(
    lookup: PrefixLookup,
    prefix: str,
) -> List[DisambiguationCandidate]:
    """Get all candidates for a prefix."""
    result = lookup.get(prefix)
    return result.candidates if result else []


def has_collision(lookup: PrefixLookup, prefix: str) -> bool:
    """Check if a prefix has collisions (multiple candidates)."""
    result = lookup.get(prefix)
    return result is not None and len(result.candidates) > 1
