"""
TX Delay Recommendations
========================

Calculates optimal transmission delay factors based on network traffic
patterns, collision risk, and node role in the mesh topology.

MeshCore uses configurable TX delay factors to reduce packet collisions:
- Flood factor: Delay multiplier for flood-routed packets (reach all nodes)
- Direct factor: Delay multiplier for direct-routed packets (point-to-point)

The goal is to stagger transmissions across the network to minimize the
chance that multiple nodes transmit simultaneously (hidden node problem).

Network Roles
-------------
    Edge Node:
        Few connections, at periphery of network. Can transmit quickly
        since collisions are less likely.
        
    Relay Node:
        Moderate connections, passes traffic between regions. Moderate
        delays needed to avoid collisions.
        
    Hub Node:
        Many connections (>15% of traffic). Critical infrastructure.
        Higher delays to let peripheral nodes transmit first.
        
    Backbone Node:
        Very high traffic share (>30%). Longest delays to coordinate
        with other backbone nodes.

Factor Alignment
----------------
MeshCore factors should align to 0.2 boundaries:
    0.2, 0.4, 0.6, 0.8, 1.0

This ensures different nodes have distinct slot timings.

API Response Format
-------------------
    GET /api/analytics/tx_recommendations
    
    Returns:
    {
        "success": true,
        "data": {
            "floodFactor": 0.6,
            "directFactor": 0.4,
            "floodSlots": 6,
            "directSlots": 4,
            "trafficIntensity": 0.45,
            "collisionRisk": "medium",
            "networkRole": "relay",
            "rationale": "Moderate traffic node - balanced delay factors"
        }
    }

See Also
--------
    - mesh-topology.ts: Frontend TxDelayRecommendation type
    - topology.py: Hub/gateway detection used for role classification
    - last_hop.py: Traffic share analysis
"""

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

from .edge_builder import parse_path
from .last_hop import get_last_hop_traffic_share

logger = logging.getLogger("Analytics.TxDelay")


class NetworkRole(Enum):
    """Node's role in the mesh network."""
    EDGE = "edge"           # Peripheral node, few connections
    RELAY = "relay"         # Mid-tier forwarding node
    HUB = "hub"             # High-traffic hub
    BACKBONE = "backbone"   # Core infrastructure


class CollisionRisk(Enum):
    """Estimated collision risk level."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


# Traffic share thresholds for role classification
BACKBONE_THRESHOLD = 0.30    # 30% of traffic
HUB_THRESHOLD = 0.15         # 15% of traffic
RELAY_THRESHOLD = 0.05       # 5% of traffic

# Factor alignment boundaries
FACTOR_STEP = 0.2


@dataclass
class TxDelayRecommendation:
    """TX delay factor recommendation."""
    flood_factor: float
    direct_factor: float
    flood_slots: int          # flood_factor * 10
    direct_slots: int         # direct_factor * 10
    traffic_intensity: float  # 0-1 normalized traffic level
    collision_risk: str       # "low", "medium", "high"
    network_role: str         # "edge", "relay", "hub", "backbone"
    rationale: str            # Human-readable explanation
    
    def to_dict(self) -> dict:
        return {
            "floodFactor": self.flood_factor,
            "directFactor": self.direct_factor,
            "floodSlots": self.flood_slots,
            "directSlots": self.direct_slots,
            "trafficIntensity": round(self.traffic_intensity, 3),
            "collisionRisk": self.collision_risk,
            "networkRole": self.network_role,
            "rationale": self.rationale,
        }


def align_to_boundary(value: float, step: float = FACTOR_STEP) -> float:
    """
    Align a value to the nearest boundary.
    
    Args:
        value: Raw value to align
        step: Step size (default 0.2)
        
    Returns:
        Aligned value
    """
    return round(value / step) * step


def classify_network_role(traffic_share: float) -> NetworkRole:
    """
    Classify node's network role based on traffic share.
    
    Args:
        traffic_share: Percentage of last-hop traffic (0-100)
        
    Returns:
        NetworkRole enum value
    """
    if traffic_share >= BACKBONE_THRESHOLD * 100:
        return NetworkRole.BACKBONE
    elif traffic_share >= HUB_THRESHOLD * 100:
        return NetworkRole.HUB
    elif traffic_share >= RELAY_THRESHOLD * 100:
        return NetworkRole.RELAY
    return NetworkRole.EDGE


def estimate_collision_risk(
    traffic_intensity: float,
    network_density: int,
) -> CollisionRisk:
    """
    Estimate collision risk based on traffic and network density.
    
    Args:
        traffic_intensity: Normalized traffic level (0-1)
        network_density: Number of active last-hop neighbors
        
    Returns:
        CollisionRisk enum value
    """
    # Combined risk score
    density_factor = min(1.0, network_density / 10)  # Normalize to ~10 neighbors
    risk_score = (traffic_intensity * 0.6) + (density_factor * 0.4)
    
    if risk_score > 0.7:
        return CollisionRisk.HIGH
    elif risk_score > 0.4:
        return CollisionRisk.MEDIUM
    return CollisionRisk.LOW


def get_tx_recommendations(
    conn,
    local_hash: Optional[str] = None,
    hours: int = 24,
) -> TxDelayRecommendation:
    """
    Calculate TX delay factor recommendations for the local node.
    
    Args:
        conn: SQLite connection
        local_hash: Local node's hash (optional, for role detection)
        hours: Time range to analyze for traffic patterns
        
    Returns:
        TxDelayRecommendation with optimized factors
    """
    # Get traffic share data
    traffic_share = get_last_hop_traffic_share(conn, hours)
    
    # Calculate traffic intensity from packet rate
    now = time.time()
    cutoff = now - (hours * 3600)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT COUNT(*) 
        FROM packets 
        WHERE timestamp > ?
    """, (cutoff,))
    total_packets = cursor.fetchone()[0]
    
    # Normalize traffic intensity (assuming ~1000 packets/day is "normal")
    packets_per_hour = total_packets / hours if hours > 0 else 0
    traffic_intensity = min(1.0, packets_per_hour / 50)  # ~50 packets/hour = high
    
    # Get local node's traffic share
    local_prefix = None
    local_share = 0.0
    if local_hash:
        from .edge_builder import get_hash_prefix
        local_prefix = get_hash_prefix(local_hash)
        local_share = traffic_share.get(local_prefix, 0.0)
    
    # Determine network role
    role = classify_network_role(local_share)
    
    # Count active last-hop neighbors (network density)
    network_density = len(traffic_share)
    
    # Estimate collision risk
    collision_risk = estimate_collision_risk(traffic_intensity, network_density)
    
    # Calculate base factors based on role
    if role == NetworkRole.BACKBONE:
        base_flood = 0.8
        base_direct = 0.6
        rationale = "Backbone node - high delays to coordinate with other infrastructure"
    elif role == NetworkRole.HUB:
        base_flood = 0.6
        base_direct = 0.4
        rationale = "Hub node - moderate-high delays to let edge nodes transmit first"
    elif role == NetworkRole.RELAY:
        base_flood = 0.4
        base_direct = 0.4
        rationale = "Relay node - balanced delays for mid-tier forwarding"
    else:  # EDGE
        base_flood = 0.2
        base_direct = 0.2
        rationale = "Edge node - low delays since collisions are less likely"
    
    # Adjust for collision risk
    if collision_risk == CollisionRisk.HIGH:
        base_flood = min(1.0, base_flood + 0.2)
        base_direct = min(1.0, base_direct + 0.2)
        rationale += " | High collision risk - increased delays"
    elif collision_risk == CollisionRisk.LOW:
        base_flood = max(0.2, base_flood - 0.1)
        rationale += " | Low collision risk"
    
    # Align to boundaries
    flood_factor = align_to_boundary(base_flood)
    direct_factor = align_to_boundary(base_direct)
    
    # Ensure minimum difference between flood and direct if node is active
    if traffic_intensity > 0.3 and flood_factor == direct_factor:
        flood_factor = min(1.0, flood_factor + 0.2)
    
    return TxDelayRecommendation(
        flood_factor=flood_factor,
        direct_factor=direct_factor,
        flood_slots=int(flood_factor * 10),
        direct_slots=int(direct_factor * 10),
        traffic_intensity=traffic_intensity,
        collision_risk=collision_risk.value,
        network_role=role.value,
        rationale=rationale,
    )


def get_network_tx_summary(
    conn,
    neighbors: Dict[str, dict],
    local_hash: Optional[str] = None,
    hours: int = 24,
) -> Dict[str, TxDelayRecommendation]:
    """
    Get TX delay recommendations for all nodes in the network.
    
    Args:
        conn: SQLite connection
        neighbors: Dict of known neighbors
        local_hash: Local node's hash
        hours: Time range to analyze
        
    Returns:
        Dict mapping node hash -> TxDelayRecommendation
    """
    # Get traffic share data
    traffic_share = get_last_hop_traffic_share(conn, hours)
    
    # Calculate overall traffic intensity
    now = time.time()
    cutoff = now - (hours * 3600)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT COUNT(*) 
        FROM packets 
        WHERE timestamp > ?
    """, (cutoff,))
    total_packets = cursor.fetchone()[0]
    packets_per_hour = total_packets / hours if hours > 0 else 0
    traffic_intensity = min(1.0, packets_per_hour / 50)
    
    # Network density
    network_density = len(traffic_share)
    collision_risk = estimate_collision_risk(traffic_intensity, network_density)
    
    recommendations = {}
    
    for neighbor_hash, neighbor in neighbors.items():
        from .edge_builder import get_hash_prefix
        prefix = get_hash_prefix(neighbor_hash)
        share = traffic_share.get(prefix, 0.0)
        
        role = classify_network_role(share)
        
        # Calculate factors
        if role == NetworkRole.BACKBONE:
            base_flood, base_direct = 0.8, 0.6
            rationale = "Backbone infrastructure"
        elif role == NetworkRole.HUB:
            base_flood, base_direct = 0.6, 0.4
            rationale = "Hub node"
        elif role == NetworkRole.RELAY:
            base_flood, base_direct = 0.4, 0.4
            rationale = "Relay node"
        else:
            base_flood, base_direct = 0.2, 0.2
            rationale = "Edge node"
        
        if collision_risk == CollisionRisk.HIGH:
            base_flood = min(1.0, base_flood + 0.2)
            base_direct = min(1.0, base_direct + 0.2)
        
        flood_factor = align_to_boundary(base_flood)
        direct_factor = align_to_boundary(base_direct)
        
        recommendations[neighbor_hash] = TxDelayRecommendation(
            flood_factor=flood_factor,
            direct_factor=direct_factor,
            flood_slots=int(flood_factor * 10),
            direct_slots=int(direct_factor * 10),
            traffic_intensity=traffic_intensity,
            collision_risk=collision_risk.value,
            network_role=role.value,
            rationale=rationale,
        )
    
    return recommendations
