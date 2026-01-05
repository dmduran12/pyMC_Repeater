"""
Geographic Utilities - Distance and proximity calculations
==========================================================

Shared geographic functions for mesh topology analysis.
Provides Haversine distance calculation and proximity scoring
tuned for large-scale LoRa mesh networks.

Proximity Bands
---------------
Distance thresholds are expanded to support networks spanning
100+ km with good antennas and elevation:

    VERY_CLOSE: < 1km   = 1.0 (direct RF neighbor)
    CLOSE:      < 6km   = 0.8
    MEDIUM:     < 24km  = 0.6
    FAR:        < 48km  = 0.4
    VERY_FAR:   < 100km = 0.2
    BEYOND:     > 100km = 0.1

Public Functions
----------------
    calculate_distance(lat1, lon1, lat2, lon2)
        Haversine distance in meters between two coordinates.
        
    get_proximity_score(distance_meters)
        Convert distance to 0-1 proximity score.
        
    has_valid_coordinates(lat, lon)
        Check if coordinates are intentionally set (not 0,0).

See Also
--------
    - prefix-disambiguation.ts: Frontend equivalent (geo-utils.ts)
    - mesh-topology.ts: Uses proximity for affinity scoring
"""

import math
from typing import Optional, Tuple

# ═══════════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════════

# Distance thresholds in meters (tuned for large LoRa mesh networks)
# These bands are expanded to support networks spanning 100+ km
PROXIMITY_BANDS = {
    "VERY_CLOSE": 1000,   # < 1km = 1.0 (very close, likely direct RF neighbor)
    "CLOSE": 6000,        # < 6km = 0.8
    "MEDIUM": 24000,      # < 24km = 0.6 (medium range)
    "FAR": 48000,         # < 48km = 0.4
    "VERY_FAR": 100000,   # < 100km = 0.2 (possible with good antennas/elevation)
}

# Proximity scores for each band
PROXIMITY_SCORES = {
    "VERY_CLOSE": 1.0,
    "CLOSE": 0.8,
    "MEDIUM": 0.6,
    "FAR": 0.4,
    "VERY_FAR": 0.2,
    "BEYOND": 0.1,
}

# Earth's radius in meters
EARTH_RADIUS_M = 6371000


# ═══════════════════════════════════════════════════════════════════════════════
# Core Functions
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_distance(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    """
    Calculate distance between two coordinates using Haversine formula.
    
    Args:
        lat1: Latitude of first point (degrees)
        lon1: Longitude of first point (degrees)
        lat2: Latitude of second point (degrees)
        lon2: Longitude of second point (degrees)
        
    Returns:
        Distance in meters
    """
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    
    a = (
        math.sin(d_lat / 2) ** 2 +
        math.cos(math.radians(lat1)) * 
        math.cos(math.radians(lat2)) *
        math.sin(d_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return EARTH_RADIUS_M * c


def get_proximity_score(distance_meters: float) -> float:
    """
    Calculate proximity score (0-1) based on distance.
    
    Closer nodes get higher scores, using LoRa-appropriate distance bands.
    
    Args:
        distance_meters: Distance in meters
        
    Returns:
        Proximity score between 0.1 and 1.0
    """
    if distance_meters < PROXIMITY_BANDS["VERY_CLOSE"]:
        return PROXIMITY_SCORES["VERY_CLOSE"]
    if distance_meters < PROXIMITY_BANDS["CLOSE"]:
        return PROXIMITY_SCORES["CLOSE"]
    if distance_meters < PROXIMITY_BANDS["MEDIUM"]:
        return PROXIMITY_SCORES["MEDIUM"]
    if distance_meters < PROXIMITY_BANDS["FAR"]:
        return PROXIMITY_SCORES["FAR"]
    if distance_meters < PROXIMITY_BANDS["VERY_FAR"]:
        return PROXIMITY_SCORES["VERY_FAR"]
    return PROXIMITY_SCORES["BEYOND"]


def has_valid_coordinates(lat: Optional[float], lon: Optional[float]) -> bool:
    """
    Check if coordinates are valid (non-zero or intentionally set).
    
    Filters out unset/default coordinates which are often (0, 0).
    
    Args:
        lat: Latitude
        lon: Longitude
        
    Returns:
        True if coordinates appear to be intentionally set
    """
    if lat is None or lon is None:
        return False
    return lat != 0 or lon != 0


def calculate_node_distance(
    node1: dict,
    node2: dict,
) -> Optional[float]:
    """
    Calculate distance between two nodes if both have valid coordinates.
    
    Args:
        node1: Dict with optional 'latitude' and 'longitude' keys
        node2: Dict with optional 'latitude' and 'longitude' keys
        
    Returns:
        Distance in meters, or None if coordinates unavailable
    """
    lat1 = node1.get("latitude")
    lon1 = node1.get("longitude")
    lat2 = node2.get("latitude")
    lon2 = node2.get("longitude")
    
    if not has_valid_coordinates(lat1, lon1):
        return None
    if not has_valid_coordinates(lat2, lon2):
        return None
    
    return calculate_distance(lat1, lon1, lat2, lon2)


def get_anchor_distance_evidence(
    candidate_lat: float,
    candidate_lon: float,
    anchor_lat: float,
    anchor_lon: float,
) -> float:
    """
    Calculate evidence score based on distance to an anchor point.
    
    Used for source-geographic and anchor correlation scoring.
    Uses the same expanded bands as PROXIMITY_BANDS for consistency.
    
    Args:
        candidate_lat: Candidate node latitude
        candidate_lon: Candidate node longitude
        anchor_lat: Anchor point latitude
        anchor_lon: Anchor point longitude
        
    Returns:
        Evidence score from 0.1 to 1.0
    """
    dist = calculate_distance(candidate_lat, candidate_lon, anchor_lat, anchor_lon)
    
    if dist < PROXIMITY_BANDS["VERY_CLOSE"]:  # < 1km
        return 1.0   # Very close - strong evidence
    elif dist < PROXIMITY_BANDS["CLOSE"]:     # < 6km
        return 0.8   # Close - good evidence
    elif dist < PROXIMITY_BANDS["MEDIUM"]:    # < 24km
        return 0.5   # Medium range - moderate evidence
    elif dist < PROXIMITY_BANDS["FAR"]:       # < 48km
        return 0.3   # Far - weak evidence
    else:
        return 0.1   # Very far - unlikely but possible via multi-hop
