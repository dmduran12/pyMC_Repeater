"""
Geographic Coverage Analysis - Spatial analysis for mesh networks
=================================================================

Analyzes the geographic distribution and coverage of mesh network nodes.
Provides coverage area calculations, gap detection, density analysis,
and optimal placement suggestions.

Key Concepts
------------
    Coverage Area:
        The geographic region covered by the mesh network, calculated
        as the convex hull of all nodes with valid coordinates.
        
    Coverage Gap:
        A region within the coverage area where node density is low
        or where adding a node would significantly improve connectivity.
        
    Node Density:
        The concentration of nodes per unit area, useful for identifying
        over-provisioned or under-provisioned regions.
        
    Optimal Placement:
        Suggested locations for new nodes that would maximize coverage
        or improve network redundancy.

Public Functions
----------------
    get_geographic_coverage(conn, min_certainty, hours)
        Get comprehensive geographic coverage analysis.
        
    calculate_coverage_area(nodes)
        Calculate the convex hull area of node positions.
        
    find_coverage_gaps(conn, nodes, grid_resolution)
        Identify regions with poor coverage.
        
    calculate_node_density(nodes, area_km2)
        Calculate node density metrics.
        
    suggest_optimal_placements(conn, nodes, num_suggestions)
        Suggest optimal locations for new nodes.

See Also
--------
    - geo_utils.py: Distance calculations used here
    - network_health.py: Uses coverage metrics for health scoring
"""

import logging
import math
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set

from .db import ensure_connection, DBConnection
from .geo_utils import (
    calculate_distance,
    has_valid_coordinates,
    EARTH_RADIUS_M,
)

logger = logging.getLogger("Analytics.GeoCoverage")

# Coverage analysis constants
MIN_NODES_FOR_HULL = 3  # Minimum nodes to form a convex hull
GRID_RESOLUTION_KM = 5.0  # Default grid cell size for gap detection
MAX_COVERAGE_RADIUS_KM = 50.0  # Maximum assumed coverage radius per node


@dataclass
class NodeLocation:
    """Node with geographic coordinates."""
    hash: str
    prefix: str
    name: Optional[str]
    latitude: float
    longitude: float
    is_repeater: bool
    neighbor_count: int = 0
    last_seen: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            "hash": self.hash,
            "prefix": self.prefix,
            "name": self.name,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "isRepeater": self.is_repeater,
            "neighborCount": self.neighbor_count,
            "lastSeen": self.last_seen,
        }


@dataclass
class CoverageGap:
    """Identified coverage gap region."""
    center_lat: float
    center_lon: float
    radius_km: float
    severity: float  # 0-1, higher = worse gap
    nearest_node_hash: Optional[str]
    nearest_node_distance_km: float
    
    def to_dict(self) -> dict:
        return {
            "centerLat": self.center_lat,
            "centerLon": self.center_lon,
            "radiusKm": round(self.radius_km, 2),
            "severity": round(self.severity, 3),
            "nearestNodeHash": self.nearest_node_hash,
            "nearestNodeDistanceKm": round(self.nearest_node_distance_km, 2),
        }


@dataclass
class PlacementSuggestion:
    """Suggested location for a new node."""
    latitude: float
    longitude: float
    priority: int  # 1 = highest priority
    reason: str
    expected_improvement: float  # 0-1
    nearby_nodes: List[str]  # Hashes of nearby nodes that would connect
    
    def to_dict(self) -> dict:
        return {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "priority": self.priority,
            "reason": self.reason,
            "expectedImprovement": round(self.expected_improvement, 3),
            "nearbyNodes": self.nearby_nodes,
        }


@dataclass
class DensityRegion:
    """Region with calculated node density."""
    center_lat: float
    center_lon: float
    node_count: int
    density_per_km2: float
    classification: str  # 'sparse', 'normal', 'dense', 'overcrowded'
    
    def to_dict(self) -> dict:
        return {
            "centerLat": self.center_lat,
            "centerLon": self.center_lon,
            "nodeCount": self.node_count,
            "densityPerKm2": round(self.density_per_km2, 4),
            "classification": self.classification,
        }


@dataclass
class ConvexHullInfo:
    """Convex hull boundary information."""
    vertices: List[Tuple[float, float]]  # (lat, lon) pairs
    area_km2: float
    perimeter_km: float
    centroid_lat: float
    centroid_lon: float
    
    def to_dict(self) -> dict:
        return {
            "vertices": [{"lat": v[0], "lon": v[1]} for v in self.vertices],
            "areaKm2": round(self.area_km2, 2),
            "perimeterKm": round(self.perimeter_km, 2),
            "centroidLat": self.centroid_lat,
            "centroidLon": self.centroid_lon,
        }


@dataclass
class GeographicCoverage:
    """Complete geographic coverage analysis."""
    timestamp: float
    
    # Node data
    nodes: List[NodeLocation]
    nodes_with_coords: int
    nodes_without_coords: int
    
    # Coverage area
    convex_hull: Optional[ConvexHullInfo]
    bounding_box: Optional[Dict[str, float]]  # minLat, maxLat, minLon, maxLon
    
    # Density analysis
    overall_density_per_km2: float
    density_regions: List[DensityRegion]
    
    # Gap analysis
    coverage_gaps: List[CoverageGap]
    coverage_score: float  # 0-1, higher = better coverage
    
    # Placement suggestions
    placement_suggestions: List[PlacementSuggestion]
    
    # Statistics
    avg_inter_node_distance_km: float
    max_inter_node_distance_km: float
    network_span_km: float  # Diameter of coverage area
    
    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "nodes": [n.to_dict() for n in self.nodes],
            "nodesWithCoords": self.nodes_with_coords,
            "nodesWithoutCoords": self.nodes_without_coords,
            "convexHull": self.convex_hull.to_dict() if self.convex_hull else None,
            "boundingBox": self.bounding_box,
            "overallDensityPerKm2": round(self.overall_density_per_km2, 4),
            "densityRegions": [r.to_dict() for r in self.density_regions],
            "coverageGaps": [g.to_dict() for g in self.coverage_gaps],
            "coverageScore": round(self.coverage_score, 3),
            "placementSuggestions": [s.to_dict() for s in self.placement_suggestions],
            "avgInterNodeDistanceKm": round(self.avg_inter_node_distance_km, 2),
            "maxInterNodeDistanceKm": round(self.max_inter_node_distance_km, 2),
            "networkSpanKm": round(self.network_span_km, 2),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# Convex Hull Algorithm (Graham Scan)
# ═══════════════════════════════════════════════════════════════════════════════

def _cross_product(o: Tuple[float, float], a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """Calculate cross product of vectors OA and OB."""
    return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])


def _compute_convex_hull(points: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    """
    Compute convex hull using Andrew's monotone chain algorithm.
    
    Args:
        points: List of (lat, lon) tuples
        
    Returns:
        List of (lat, lon) vertices forming the convex hull in counter-clockwise order
    """
    if len(points) < 3:
        return points
    
    # Sort points lexicographically
    points = sorted(set(points))
    
    if len(points) < 3:
        return points
    
    # Build lower hull
    lower = []
    for p in points:
        while len(lower) >= 2 and _cross_product(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)
    
    # Build upper hull
    upper = []
    for p in reversed(points):
        while len(upper) >= 2 and _cross_product(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)
    
    # Remove last point of each half because it's repeated
    return lower[:-1] + upper[:-1]


def _calculate_polygon_area(vertices: List[Tuple[float, float]]) -> float:
    """
    Calculate approximate area of a polygon on Earth's surface.
    
    Uses the Shoelace formula with latitude correction.
    Returns area in square kilometers.
    """
    if len(vertices) < 3:
        return 0.0
    
    # Calculate centroid latitude for area correction
    avg_lat = sum(v[0] for v in vertices) / len(vertices)
    lat_correction = math.cos(math.radians(avg_lat))
    
    # Shoelace formula
    n = len(vertices)
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        # Convert to approximate km (1 degree ≈ 111 km)
        x1 = vertices[i][1] * 111.0 * lat_correction
        y1 = vertices[i][0] * 111.0
        x2 = vertices[j][1] * 111.0 * lat_correction
        y2 = vertices[j][0] * 111.0
        area += x1 * y2 - x2 * y1
    
    return abs(area) / 2.0


def _calculate_polygon_perimeter(vertices: List[Tuple[float, float]]) -> float:
    """Calculate perimeter of polygon in kilometers."""
    if len(vertices) < 2:
        return 0.0
    
    perimeter = 0.0
    n = len(vertices)
    for i in range(n):
        j = (i + 1) % n
        dist = calculate_distance(
            vertices[i][0], vertices[i][1],
            vertices[j][0], vertices[j][1]
        )
        perimeter += dist / 1000.0  # Convert to km
    
    return perimeter


# ═══════════════════════════════════════════════════════════════════════════════
# Core Analysis Functions
# ═══════════════════════════════════════════════════════════════════════════════

def _get_nodes_with_coordinates(
    conn: DBConnection,
    hours: int = 168,
) -> Tuple[List[NodeLocation], int]:
    """
    Get all nodes that have valid geographic coordinates.
    
    Returns:
        Tuple of (nodes with coords, count of nodes without coords)
    """
    cutoff = time.time() - (hours * 3600)
    
    cursor = conn.execute("""
        SELECT 
            pubkey,
            node_name,
            latitude,
            longitude,
            is_repeater,
            last_seen
        FROM adverts
        WHERE last_seen > ?
        GROUP BY pubkey
        HAVING MAX(last_seen)
    """, (cutoff,))
    
    nodes_with_coords = []
    nodes_without_coords = 0
    
    for row in cursor:
        lat = row["latitude"]
        lon = row["longitude"]
        
        if has_valid_coordinates(lat, lon):
            # Get prefix from hash
            pubkey = row["pubkey"]
            prefix = pubkey[2:4] if pubkey.startswith("0x") else pubkey[:2]
            
            nodes_with_coords.append(NodeLocation(
                hash=pubkey,
                prefix=prefix.upper(),
                name=row["node_name"],
                latitude=lat,
                longitude=lon,
                is_repeater=bool(row["is_repeater"]),
                last_seen=row["last_seen"],
            ))
        else:
            nodes_without_coords += 1
    
    return nodes_with_coords, nodes_without_coords


def _calculate_bounding_box(nodes: List[NodeLocation]) -> Optional[Dict[str, float]]:
    """Calculate bounding box of node positions."""
    if not nodes:
        return None
    
    lats = [n.latitude for n in nodes]
    lons = [n.longitude for n in nodes]
    
    return {
        "minLat": min(lats),
        "maxLat": max(lats),
        "minLon": min(lons),
        "maxLon": max(lons),
    }


def _calculate_inter_node_distances(nodes: List[NodeLocation]) -> Tuple[float, float]:
    """
    Calculate average and maximum inter-node distances.
    
    Returns:
        Tuple of (avg_distance_km, max_distance_km)
    """
    if len(nodes) < 2:
        return 0.0, 0.0
    
    distances = []
    max_dist = 0.0
    
    for i, n1 in enumerate(nodes):
        for n2 in nodes[i+1:]:
            dist = calculate_distance(
                n1.latitude, n1.longitude,
                n2.latitude, n2.longitude
            ) / 1000.0  # Convert to km
            distances.append(dist)
            max_dist = max(max_dist, dist)
    
    avg_dist = sum(distances) / len(distances) if distances else 0.0
    return avg_dist, max_dist


def _build_convex_hull(nodes: List[NodeLocation]) -> Optional[ConvexHullInfo]:
    """Build convex hull from node positions."""
    if len(nodes) < MIN_NODES_FOR_HULL:
        return None
    
    points = [(n.latitude, n.longitude) for n in nodes]
    hull_vertices = _compute_convex_hull(points)
    
    if len(hull_vertices) < 3:
        return None
    
    area = _calculate_polygon_area(hull_vertices)
    perimeter = _calculate_polygon_perimeter(hull_vertices)
    
    # Calculate centroid
    centroid_lat = sum(v[0] for v in hull_vertices) / len(hull_vertices)
    centroid_lon = sum(v[1] for v in hull_vertices) / len(hull_vertices)
    
    return ConvexHullInfo(
        vertices=hull_vertices,
        area_km2=area,
        perimeter_km=perimeter,
        centroid_lat=centroid_lat,
        centroid_lon=centroid_lon,
    )


def _find_coverage_gaps(
    nodes: List[NodeLocation],
    bounding_box: Dict[str, float],
    grid_resolution_km: float = GRID_RESOLUTION_KM,
    coverage_radius_km: float = MAX_COVERAGE_RADIUS_KM,
) -> List[CoverageGap]:
    """
    Find coverage gaps by analyzing a grid over the coverage area.
    
    Creates a grid of points and identifies areas where the nearest
    node is beyond the expected coverage radius.
    """
    if not nodes or not bounding_box:
        return []
    
    gaps = []
    
    # Calculate grid step in degrees (approximate)
    lat_step = grid_resolution_km / 111.0  # 1 degree ≈ 111 km
    avg_lat = (bounding_box["minLat"] + bounding_box["maxLat"]) / 2
    lon_step = grid_resolution_km / (111.0 * math.cos(math.radians(avg_lat)))
    
    # Add padding to bounding box
    padding = grid_resolution_km / 111.0
    min_lat = bounding_box["minLat"] - padding
    max_lat = bounding_box["maxLat"] + padding
    min_lon = bounding_box["minLon"] - padding * math.cos(math.radians(avg_lat))
    max_lon = bounding_box["maxLon"] + padding * math.cos(math.radians(avg_lat))
    
    # Iterate through grid
    lat = min_lat
    while lat <= max_lat:
        lon = min_lon
        while lon <= max_lon:
            # Find nearest node
            min_dist = float('inf')
            nearest_hash = None
            
            for node in nodes:
                dist = calculate_distance(lat, lon, node.latitude, node.longitude) / 1000.0
                if dist < min_dist:
                    min_dist = dist
                    nearest_hash = node.hash
            
            # Check if this is a gap
            if min_dist > coverage_radius_km * 0.5:  # Gap if > 50% of coverage radius
                severity = min(1.0, min_dist / coverage_radius_km)
                gaps.append(CoverageGap(
                    center_lat=lat,
                    center_lon=lon,
                    radius_km=grid_resolution_km / 2,
                    severity=severity,
                    nearest_node_hash=nearest_hash,
                    nearest_node_distance_km=min_dist,
                ))
            
            lon += lon_step
        lat += lat_step
    
    # Merge nearby gaps and return top gaps by severity
    gaps.sort(key=lambda g: g.severity, reverse=True)
    return gaps[:20]  # Return top 20 gaps


def _calculate_density_regions(
    nodes: List[NodeLocation],
    bounding_box: Dict[str, float],
    grid_resolution_km: float = GRID_RESOLUTION_KM * 2,
) -> List[DensityRegion]:
    """
    Calculate node density in different regions.
    
    Divides the coverage area into regions and calculates
    the node density in each.
    """
    if not nodes or not bounding_box:
        return []
    
    regions = []
    
    # Calculate grid parameters
    lat_step = grid_resolution_km / 111.0
    avg_lat = (bounding_box["minLat"] + bounding_box["maxLat"]) / 2
    lon_step = grid_resolution_km / (111.0 * math.cos(math.radians(avg_lat)))
    
    cell_area_km2 = grid_resolution_km ** 2
    
    # Density thresholds (nodes per km²)
    SPARSE_THRESHOLD = 0.01
    DENSE_THRESHOLD = 0.1
    OVERCROWDED_THRESHOLD = 0.5
    
    lat = bounding_box["minLat"]
    while lat <= bounding_box["maxLat"]:
        lon = bounding_box["minLon"]
        while lon <= bounding_box["maxLon"]:
            # Count nodes in this cell
            count = 0
            for node in nodes:
                if (lat <= node.latitude < lat + lat_step and
                    lon <= node.longitude < lon + lon_step):
                    count += 1
            
            if count > 0:  # Only include cells with nodes
                density = count / cell_area_km2
                
                if density < SPARSE_THRESHOLD:
                    classification = "sparse"
                elif density < DENSE_THRESHOLD:
                    classification = "normal"
                elif density < OVERCROWDED_THRESHOLD:
                    classification = "dense"
                else:
                    classification = "overcrowded"
                
                regions.append(DensityRegion(
                    center_lat=lat + lat_step / 2,
                    center_lon=lon + lon_step / 2,
                    node_count=count,
                    density_per_km2=density,
                    classification=classification,
                ))
            
            lon += lon_step
        lat += lat_step
    
    return regions


def _suggest_placements(
    nodes: List[NodeLocation],
    gaps: List[CoverageGap],
    num_suggestions: int = 5,
) -> List[PlacementSuggestion]:
    """
    Suggest optimal locations for new nodes.
    
    Uses coverage gaps and connectivity analysis to suggest
    locations that would improve the network.
    """
    if not gaps:
        return []
    
    suggestions = []
    
    # Sort gaps by severity
    sorted_gaps = sorted(gaps, key=lambda g: g.severity, reverse=True)
    
    for i, gap in enumerate(sorted_gaps[:num_suggestions]):
        # Find nodes that would be within range
        nearby_nodes = []
        for node in nodes:
            dist = calculate_distance(
                gap.center_lat, gap.center_lon,
                node.latitude, node.longitude
            ) / 1000.0
            if dist < MAX_COVERAGE_RADIUS_KM:
                nearby_nodes.append(node.hash)
        
        # Determine reason based on gap characteristics
        if gap.nearest_node_distance_km > MAX_COVERAGE_RADIUS_KM:
            reason = "Extend network coverage to unreached area"
        elif gap.severity > 0.7:
            reason = "Fill significant coverage gap"
        else:
            reason = "Improve network density and redundancy"
        
        suggestions.append(PlacementSuggestion(
            latitude=gap.center_lat,
            longitude=gap.center_lon,
            priority=i + 1,
            reason=reason,
            expected_improvement=gap.severity * 0.8,  # Estimate
            nearby_nodes=nearby_nodes[:5],  # Top 5 nearby nodes
        ))
    
    return suggestions


def _calculate_coverage_score(
    nodes: List[NodeLocation],
    gaps: List[CoverageGap],
    hull: Optional[ConvexHullInfo],
) -> float:
    """
    Calculate overall coverage score (0-1).
    
    Higher score = better coverage.
    """
    if not nodes:
        return 0.0
    
    # Base score from node count
    node_score = min(1.0, len(nodes) / 20.0)  # Cap at 20 nodes for full score
    
    # Gap penalty
    if gaps:
        avg_severity = sum(g.severity for g in gaps) / len(gaps)
        gap_penalty = avg_severity * 0.5
    else:
        gap_penalty = 0.0
    
    # Area bonus (larger networks get slight bonus)
    if hull and hull.area_km2 > 0:
        area_bonus = min(0.1, hull.area_km2 / 10000.0)  # Max 0.1 bonus
    else:
        area_bonus = 0.0
    
    score = node_score - gap_penalty + area_bonus
    return max(0.0, min(1.0, score))


# ═══════════════════════════════════════════════════════════════════════════════
# Main API Function
# ═══════════════════════════════════════════════════════════════════════════════

@ensure_connection
def get_geographic_coverage(
    conn: DBConnection,
    hours: int = 168,
    grid_resolution_km: float = GRID_RESOLUTION_KM,
    num_placement_suggestions: int = 5,
) -> GeographicCoverage:
    """
    Get comprehensive geographic coverage analysis.
    
    Args:
        conn: Database connection
        hours: Time range for node activity (default: 7 days)
        grid_resolution_km: Grid cell size for analysis
        num_placement_suggestions: Number of placement suggestions to generate
        
    Returns:
        GeographicCoverage with full analysis
    """
    # Get nodes with coordinates
    nodes, nodes_without = _get_nodes_with_coordinates(conn, hours)
    
    # Calculate bounding box
    bounding_box = _calculate_bounding_box(nodes)
    
    # Build convex hull
    hull = _build_convex_hull(nodes)
    
    # Calculate inter-node distances
    avg_dist, max_dist = _calculate_inter_node_distances(nodes)
    
    # Find coverage gaps
    gaps = []
    if bounding_box:
        gaps = _find_coverage_gaps(nodes, bounding_box, grid_resolution_km)
    
    # Calculate density regions
    density_regions = []
    if bounding_box:
        density_regions = _calculate_density_regions(nodes, bounding_box)
    
    # Calculate overall density
    if hull and hull.area_km2 > 0:
        overall_density = len(nodes) / hull.area_km2
    else:
        overall_density = 0.0
    
    # Suggest placements
    suggestions = _suggest_placements(nodes, gaps, num_placement_suggestions)
    
    # Calculate coverage score
    coverage_score = _calculate_coverage_score(nodes, gaps, hull)
    
    # Calculate network span
    network_span = max_dist  # Diameter is the max inter-node distance
    
    return GeographicCoverage(
        timestamp=time.time(),
        nodes=nodes,
        nodes_with_coords=len(nodes),
        nodes_without_coords=nodes_without,
        convex_hull=hull,
        bounding_box=bounding_box,
        overall_density_per_km2=overall_density,
        density_regions=density_regions,
        coverage_gaps=gaps,
        coverage_score=coverage_score,
        placement_suggestions=suggestions,
        avg_inter_node_distance_km=avg_dist,
        max_inter_node_distance_km=max_dist,
        network_span_km=network_span,
    )


@ensure_connection
def get_coverage_summary(
    conn: DBConnection,
    hours: int = 168,
) -> dict:
    """
    Get a quick coverage summary without full analysis.
    
    Lighter-weight alternative to get_geographic_coverage().
    """
    nodes, nodes_without = _get_nodes_with_coordinates(conn, hours)
    hull = _build_convex_hull(nodes)
    avg_dist, max_dist = _calculate_inter_node_distances(nodes)
    
    return {
        "nodesWithCoords": len(nodes),
        "nodesWithoutCoords": nodes_without,
        "areaKm2": hull.area_km2 if hull else 0.0,
        "avgInterNodeDistanceKm": round(avg_dist, 2),
        "maxInterNodeDistanceKm": round(max_dist, 2),
        "hasEnoughNodes": len(nodes) >= MIN_NODES_FOR_HULL,
    }
