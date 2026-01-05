"""
Edge Builder - Extract topology edges from packet paths
========================================================

Analyzes packet forwarding paths to infer RF links between nodes.
Consecutive prefixes in a path represent actual observed hops.

This module is the foundation for topology visualization - it transforms
raw packet path data into validated network edges that can be rendered
as a graph.

Key Concepts
------------
    Edge Key:
        Canonical identifier for an edge, formed by sorting the two
        endpoint hashes alphabetically. This ensures A->B and B->A
        refer to the same edge: "0xAB:0xCD" (never "0xCD:0xAB").
        
    Certain Edge:
        An edge where both endpoints are resolved with high confidence.
        Requires MIN_EDGE_VALIDATIONS (15) certain observations before
        appearing in topology views.
        
    Observation:
        Single instance of an edge being traversed in a packet path.
        Each observation contributes to the edge's confidence score.
        
    Confidence Scoring:
        - 0.95: Both endpoints are known neighbors
        - 0.80: Destination is a known neighbor  
        - 0.70: Source is a known neighbor
        - 0.50: Neither endpoint is known

Path Analysis Algorithm
-----------------------
Given a packet with path [A, B, C] received at local node L:

    1. Source -> First Hop: If src_hash is known, infer edge src->A
    2. Consecutive Pairs: Infer edges A->B and B->C
    3. Last Hop -> Local: Infer edge C->L (highest confidence)

Database Schema
---------------
    edge_observations:
        - from_hash, to_hash: Canonical endpoint pair
        - edge_key: Unique identifier (sorted hash pair)
        - observation_count: Total observations
        - certain_count: High-confidence observations
        - confidence_sum: Sum for averaging
        - forward_count, reverse_count: Directional traffic
        - first_seen, last_seen: Timestamps

Public Functions
----------------
    extract_edges_from_packet(packet, local_hash, neighbors)
        Extract EdgeObservation objects from a single packet.
        
    update_edge_observations(conn, observations)
        Upsert edge observations into database.
        
    get_validated_edges(conn, min_certain_count, limit)
        Query edges meeting validation threshold.

Example
-------
    >>> packet = {
    ...     'original_path': ['AB', 'CD', 'EF'],
    ...     'src_hash': '0x12345678',
    ...     'timestamp': time.time()
    ... }
    >>> edges = extract_edges_from_packet(packet, local_hash='0xGH')
    >>> len(edges)  # src->AB, AB->CD, CD->EF, EF->local
    4

See Also
--------
    - topology.py: Uses validated edges for graph analysis
    - worker.py: Calls extract_edges_from_packet on each packet
"""

import json
import logging
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

logger = logging.getLogger("Analytics.EdgeBuilder")


@dataclass
class EdgeObservation:
    """Single observation of an edge being traversed."""
    from_hash: str
    to_hash: str
    edge_key: str
    confidence: float
    is_certain: bool
    hop_distance: int  # Distance from local node (0 = last hop)
    timestamp: float = field(default_factory=time.time)
    route_type: Optional[int] = None
    is_flood: bool = False       # True if packet was flood routed
    is_direct: bool = False      # True if packet was direct routed
    disambiguation_conf: float = 0.0  # Confidence from disambiguation system


def make_edge_key(hash_a: str, hash_b: str) -> str:
    """
    Create canonical edge key from two hashes.
    
    Edge keys are always sorted alphabetically to ensure
    consistent lookup regardless of traversal direction.
    
    Args:
        hash_a: First node hash (e.g., "0xABCD1234")
        hash_b: Second node hash
        
    Returns:
        Canonical edge key (e.g., "0xABCD1234:0xEF567890")
    """
    normalized_a = hash_a.upper()
    normalized_b = hash_b.upper()
    
    if normalized_a < normalized_b:
        return f"{normalized_a}:{normalized_b}"
    return f"{normalized_b}:{normalized_a}"


def parse_path(raw_path) -> Optional[List[str]]:
    """
    Parse packet path from various formats.
    
    Handles:
        - JSON string: '["AB", "CD", "EF"]'
        - List of strings: ["AB", "CD", "EF"]
        - List of ints: [0xAB, 0xCD, 0xEF]
        - None/empty
        
    Returns:
        List of uppercase 2-char prefixes, or None if invalid
    """
    if not raw_path:
        return None
    
    # Already a list
    if isinstance(raw_path, (list, tuple)):
        path = raw_path
    # JSON string
    elif isinstance(raw_path, str):
        try:
            path = json.loads(raw_path)
            if not isinstance(path, list):
                return None
        except (json.JSONDecodeError, ValueError):
            return None
    else:
        return None
    
    # Normalize to uppercase strings
    result = []
    for hop in path:
        if isinstance(hop, int):
            result.append(f"{hop:02X}")
        elif isinstance(hop, str):
            result.append(hop.upper())
        else:
            continue
    
    return result if result else None


def get_hash_prefix(full_hash: str) -> str:
    """
    Extract 2-char prefix from full hash.
    
    Handles formats:
        - "0xABCDEF12" -> "AB"
        - "ABCDEF12" -> "AB"
        - "AB" -> "AB"
    """
    if not full_hash:
        return ""
    
    h = full_hash.upper()
    if h.startswith("0X"):
        h = h[2:]
    
    return h[:2] if len(h) >= 2 else h


def extract_edges_from_packet(
    packet: dict,
    local_hash: Optional[str] = None,
    neighbors: Optional[dict] = None,
) -> List[EdgeObservation]:
    """
    Extract edge observations from a single packet.
    
    Analyzes the packet's forwarding path to identify RF links.
    Consecutive prefixes in the path represent observed hops.
    
    Args:
        packet: Packet record dict with original_path, forwarded_path, src_hash
        local_hash: Local node's full hash (for last-hop detection)
        neighbors: Known neighbors dict for confidence boosting
        
    Returns:
        List of EdgeObservation objects for each inferred edge
    """
    edges = []
    
    # Get path (prefer forwarded_path, fall back to original_path)
    raw_path = packet.get("forwarded_path") or packet.get("original_path")
    path = parse_path(raw_path)
    
    if not path:
        return edges
    
    timestamp = packet.get("timestamp", time.time())
    route_type = packet.get("route") or packet.get("route_type")
    src_hash = packet.get("src_hash")
    
    # Determine if flood or direct route
    # Route type 0 = flood, 1 = direct (varies by implementation)
    is_flood = route_type == 0 or route_type is None  # Default to flood if unknown
    is_direct = route_type == 1
    
    local_prefix = get_hash_prefix(local_hash) if local_hash else None
    neighbors = neighbors or {}
    
    # Build set of known neighbor prefixes for confidence boosting
    neighbor_prefixes = set()
    for neighbor_hash in neighbors.keys():
        prefix = get_hash_prefix(neighbor_hash)
        if prefix:
            neighbor_prefixes.add(prefix)
    
    path_len = len(path)
    
    # === SOURCE -> FIRST HOP ===
    # If we have src_hash and a non-empty path, infer src -> first_hop edge
    if src_hash and path_len >= 1:
        first_hop_prefix = path[0]
        src_prefix = get_hash_prefix(src_hash)
        
        if first_hop_prefix and src_prefix and first_hop_prefix != src_prefix:
            # Confidence based on whether src is known
            src_known = src_hash in neighbors
            confidence = 0.9 if src_known else 0.6
            is_certain = src_known
            
            edge_key = make_edge_key(src_hash, f"0x{first_hop_prefix}")
            edges.append(EdgeObservation(
                from_hash=src_hash,
                to_hash=f"0x{first_hop_prefix}",
                edge_key=edge_key,
                confidence=confidence,
                is_certain=is_certain,
                hop_distance=path_len,
                timestamp=timestamp,
                route_type=route_type,
                is_flood=is_flood,
                is_direct=is_direct,
            ))
    
    # === CONSECUTIVE PATH PAIRS ===
    for i in range(path_len - 1):
        from_prefix = path[i]
        to_prefix = path[i + 1]
        
        if not from_prefix or not to_prefix:
            continue
        if from_prefix == to_prefix:
            continue
        
        # Calculate hop distance from local (0 = last hop to local)
        # Position in path: i is at distance (path_len - 1 - i) from end
        hop_distance = path_len - 1 - i
        
        # Confidence scoring
        from_known = from_prefix in neighbor_prefixes
        to_known = to_prefix in neighbor_prefixes
        
        if from_known and to_known:
            confidence = 0.95
            is_certain = True
        elif to_known:
            confidence = 0.8
            is_certain = hop_distance == 0  # Certain if last hop to us
        elif from_known:
            confidence = 0.7
            is_certain = False
        else:
            confidence = 0.5
            is_certain = False
        
        from_hash = f"0x{from_prefix}"
        to_hash = f"0x{to_prefix}"
        edge_key = make_edge_key(from_hash, to_hash)
        
        edges.append(EdgeObservation(
            from_hash=from_hash,
            to_hash=to_hash,
            edge_key=edge_key,
            confidence=confidence,
            is_certain=is_certain,
            hop_distance=hop_distance,
            timestamp=timestamp,
            route_type=route_type,
            is_flood=is_flood,
            is_direct=is_direct,
        ))
    
    # === LAST HOP -> LOCAL ===
    if local_hash and path_len >= 1:
        last_hop_prefix = path[-1]
        
        if last_hop_prefix and last_hop_prefix != local_prefix:
            # Last hop to local is always high confidence
            confidence = 0.95
            is_certain = True
            
            last_hop_hash = f"0x{last_hop_prefix}"
            edge_key = make_edge_key(last_hop_hash, local_hash)
            
            edges.append(EdgeObservation(
                from_hash=last_hop_hash,
                to_hash=local_hash,
                edge_key=edge_key,
                confidence=confidence,
                is_certain=is_certain,
                hop_distance=0,
                timestamp=timestamp,
                route_type=route_type,
                is_flood=is_flood,
                is_direct=is_direct,
            ))
    
    return edges


def update_edge_observations(
    conn,
    observations: List[EdgeObservation],
) -> int:
    """
    Update edge_observations table with new observations.
    
    Uses upsert pattern to increment counts and update timestamps.
    
    Args:
        conn: SQLite connection
        observations: List of EdgeObservation objects
        
    Returns:
        Number of edges updated
    """
    if not observations:
        return 0
    
    cursor = conn.cursor()
    updated = 0
    
    for obs in observations:
        try:
            # Determine canonical direction
            if obs.from_hash < obs.to_hash:
                from_h, to_h = obs.from_hash, obs.to_hash
                is_forward = True
            else:
                from_h, to_h = obs.to_hash, obs.from_hash
                is_forward = False
            
            # Upsert edge observation
            cursor.execute("""
                INSERT INTO edge_observations (
                    from_hash, to_hash, edge_key,
                    observation_count, certain_count, confidence_sum,
                    forward_count, reverse_count,
                    first_seen, last_seen
                ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(edge_key) DO UPDATE SET
                    observation_count = observation_count + 1,
                    certain_count = certain_count + excluded.certain_count,
                    confidence_sum = confidence_sum + excluded.confidence_sum,
                    forward_count = forward_count + excluded.forward_count,
                    reverse_count = reverse_count + excluded.reverse_count,
                    last_seen = excluded.last_seen
            """, (
                from_h,
                to_h,
                obs.edge_key,
                1 if obs.is_certain else 0,
                obs.confidence,
                1 if is_forward else 0,
                0 if is_forward else 1,
                obs.timestamp,
                obs.timestamp,
            ))
            updated += 1
            
        except Exception as e:
            logger.error(f"Failed to update edge {obs.edge_key}: {e}")
            continue
    
    conn.commit()
    return updated


def get_validated_edges(
    conn,
    min_certain_count: int = 15,
    limit: int = 500,
) -> List[dict]:
    """
    Get validated topology edges from database.
    
    Returns edges that have been observed with sufficient certainty.
    
    Args:
        conn: SQLite connection
        min_certain_count: Minimum certain observations to include
        limit: Maximum edges to return
        
    Returns:
        List of edge dicts with computed metrics
    """
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            from_hash,
            to_hash,
            edge_key,
            observation_count,
            certain_count,
            confidence_sum,
            forward_count,
            reverse_count,
            first_seen,
            last_seen
        FROM edge_observations
        WHERE certain_count >= ?
        ORDER BY certain_count DESC
        LIMIT ?
    """, (min_certain_count, limit))
    
    edges = []
    for row in cursor.fetchall():
        obs_count = row[3]
        certain_count = row[4]
        confidence_sum = row[5]
        forward_count = row[6]
        reverse_count = row[7]
        
        # Calculate derived metrics
        avg_confidence = confidence_sum / obs_count if obs_count > 0 else 0
        
        total_directional = forward_count + reverse_count
        symmetry_ratio = (
            min(forward_count, reverse_count) / max(forward_count, reverse_count)
            if total_directional > 0 and max(forward_count, reverse_count) > 0
            else 0
        )
        
        edges.append({
            "fromHash": row[0],
            "toHash": row[1],
            "edgeKey": row[2],
            "packetCount": obs_count,
            "certainCount": certain_count,
            "avgConfidence": round(avg_confidence, 3),
            "forwardCount": forward_count,
            "reverseCount": reverse_count,
            "symmetryRatio": round(symmetry_ratio, 3),
            "firstSeen": row[8],
            "lastSeen": row[9],
        })
    
    return edges
