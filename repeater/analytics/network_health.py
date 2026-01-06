"""
Network Health - Overall mesh network health dashboard
======================================================

Provides a comprehensive health assessment of the entire mesh network,
including connectivity metrics, issue detection, and actionable
recommendations for improving network reliability.

Health Score Components
-----------------------
    Connectivity (25%):
        - Are all nodes reachable from the local node?
        - How many isolated nodes/components exist?
        
    Redundancy (25%):
        - Do critical paths have backup routes?
        - Edge redundancy ratio (actual/minimum edges)
        
    Stability (25%):
        - Are paths consistent over time?
        - Low path volatility indicates stability
        
    Coverage (25%):
        - Geographic distribution of nodes
        - Signal quality distribution

Issue Detection
---------------
    Single Points of Failure:
        - Articulation points (critical nodes)
        - Bridge edges (critical links)
        
    Weak Links:
        - Edges with low confidence scores
        - Asymmetric links (one-way traffic)
        
    Overloaded Nodes:
        - Nodes handling disproportionate traffic
        - Hub nodes without redundancy
        
    Network Fragmentation:
        - Multiple disconnected components
        - Isolated nodes

Recommendations
---------------
Based on detected issues, the module generates actionable
recommendations such as:
    - Add redundant paths to critical nodes
    - Investigate weak links
    - Consider adding relay nodes in coverage gaps

API Endpoint
------------
    GET /api/analytics/network_health
    
    Returns NetworkHealthReport with:
        - Overall health score (0-1)
        - Component scores
        - Detected issues
        - Recommendations

See Also
--------
    - graph.py: Core graph analysis algorithms
    - topology.py: Hub/gateway detection
    - node_profile.py: Individual node analysis
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum

from .db import AnalyticsDB, ensure_connection, DBConnection
from .graph import build_graph_from_db, MeshGraph, NETWORKX_AVAILABLE
from .topology import build_topology_from_db, TopologySnapshot
from .edge_builder import get_hash_prefix

logger = logging.getLogger("Analytics.NetworkHealth")

# Health thresholds
CRITICAL_NODE_TRAFFIC_THRESHOLD = 0.3  # 30% of traffic = critical
WEAK_LINK_CONFIDENCE_THRESHOLD = 0.4   # Links below 40% confidence
ASYMMETRIC_LINK_THRESHOLD = 0.3        # Symmetry ratio below 30%
HIGH_PATH_VOLATILITY_THRESHOLD = 0.6   # Path changes above 60%


class IssueSeverity(str, Enum):
    """Severity levels for detected issues."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class IssueType(str, Enum):
    """Types of network health issues."""
    SINGLE_POINT_OF_FAILURE = "singlePointOfFailure"
    BRIDGE_EDGE = "bridgeEdge"
    WEAK_LINK = "weakLink"
    ASYMMETRIC_LINK = "asymmetricLink"
    OVERLOADED_NODE = "overloadedNode"
    ISOLATED_NODE = "isolatedNode"
    FRAGMENTED_NETWORK = "fragmentedNetwork"
    LOW_REDUNDANCY = "lowRedundancy"
    COVERAGE_GAP = "coverageGap"


@dataclass
class NetworkIssue:
    """A detected network health issue."""
    type: IssueType
    severity: IssueSeverity
    node: Optional[str] = None
    edge: Optional[str] = None
    description: str = ""
    metric_value: Optional[float] = None
    
    def to_dict(self) -> dict:
        return {
            "type": self.type.value,
            "severity": self.severity.value,
            "node": self.node,
            "edge": self.edge,
            "description": self.description,
            "metricValue": round(self.metric_value, 3) if self.metric_value else None,
        }


@dataclass
class HealthMetrics:
    """Component health metrics."""
    connectivity: float = 0.0      # 0-1: network connectivity
    redundancy: float = 0.0        # 0-1: path redundancy
    stability: float = 0.0         # 0-1: path stability over time
    coverage: float = 0.0          # 0-1: geographic/signal coverage
    
    def to_dict(self) -> dict:
        return {
            "connectivity": round(self.connectivity, 3),
            "redundancy": round(self.redundancy, 3),
            "stability": round(self.stability, 3),
            "coverage": round(self.coverage, 3),
        }


@dataclass
class Recommendation:
    """An actionable recommendation for improving network health."""
    priority: int  # 1 = highest
    title: str
    description: str
    related_nodes: List[str] = field(default_factory=list)
    related_edges: List[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            "priority": self.priority,
            "title": self.title,
            "description": self.description,
            "relatedNodes": self.related_nodes,
            "relatedEdges": self.related_edges,
        }


@dataclass
class NetworkHealthReport:
    """Complete network health report."""
    health_score: float
    metrics: HealthMetrics
    issues: List[NetworkIssue] = field(default_factory=list)
    recommendations: List[Recommendation] = field(default_factory=list)
    
    # Summary stats
    total_nodes: int = 0
    total_edges: int = 0
    hub_count: int = 0
    articulation_point_count: int = 0
    bridge_count: int = 0
    component_count: int = 1
    
    # Timestamps
    computed_at: float = field(default_factory=time.time)
    analysis_duration_ms: float = 0.0
    
    def to_dict(self) -> dict:
        return {
            "healthScore": round(self.health_score, 3),
            "metrics": self.metrics.to_dict(),
            "issues": [i.to_dict() for i in self.issues],
            "recommendations": [r.to_dict() for r in self.recommendations],
            "totalNodes": self.total_nodes,
            "totalEdges": self.total_edges,
            "hubCount": self.hub_count,
            "articulationPointCount": self.articulation_point_count,
            "bridgeCount": self.bridge_count,
            "componentCount": self.component_count,
            "computedAt": self.computed_at,
            "analysisDurationMs": round(self.analysis_duration_ms, 1),
        }


def _calculate_connectivity_score(graph: MeshGraph) -> Tuple[float, List[NetworkIssue]]:
    """Calculate connectivity health score and detect issues."""
    issues = []
    
    if graph.node_count == 0:
        return 0.0, issues
    
    if graph.node_count == 1:
        return 1.0, issues
    
    components = graph.connected_components()
    
    # Perfect connectivity = 1 component
    if components.total_components == 1:
        connectivity = 1.0
    else:
        # Score based on largest component ratio
        largest_ratio = components.largest_component_size / components.total_nodes
        connectivity = largest_ratio * 0.7  # Max 0.7 if fragmented
        
        # Add fragmentation issue
        issues.append(NetworkIssue(
            type=IssueType.FRAGMENTED_NETWORK,
            severity=IssueSeverity.HIGH if components.total_components > 2 else IssueSeverity.MEDIUM,
            description=f"Network is fragmented into {components.total_components} components",
            metric_value=components.total_components,
        ))
    
    # Add issues for isolated nodes
    for node in components.isolated_nodes:
        issues.append(NetworkIssue(
            type=IssueType.ISOLATED_NODE,
            severity=IssueSeverity.LOW,
            node=node,
            description=f"Node {get_hash_prefix(node)} is isolated from the network",
        ))
    
    return connectivity, issues


def _calculate_redundancy_score(graph: MeshGraph) -> Tuple[float, List[NetworkIssue]]:
    """Calculate redundancy health score and detect issues."""
    issues = []
    
    if graph.node_count < 2:
        return 1.0 if graph.node_count <= 1 else 0.0, issues
    
    # Edge redundancy: compare actual edges to minimum (spanning tree)
    min_edges = graph.node_count - 1
    actual_edges = graph.edge_count // 2  # Divide by 2 for undirected
    
    if min_edges == 0:
        return 1.0, issues
    
    # Redundancy ratio (1.0 = just enough, 2.0 = double coverage, etc.)
    redundancy_ratio = actual_edges / min_edges
    
    # Score: 0 at 1x, 0.5 at 1.5x, 1.0 at 2x+
    redundancy = min(1.0, max(0, (redundancy_ratio - 1.0)))
    
    if redundancy < 0.3:
        issues.append(NetworkIssue(
            type=IssueType.LOW_REDUNDANCY,
            severity=IssueSeverity.MEDIUM,
            description=f"Network has low path redundancy (ratio: {redundancy_ratio:.1f}x minimum)",
            metric_value=redundancy_ratio,
        ))
    
    # Check for bridges (critical edges)
    bridges = graph.find_bridges()
    for u, v in bridges:
        edge_key = graph.get_edge_key(u, v) or f"{u}:{v}"
        issues.append(NetworkIssue(
            type=IssueType.BRIDGE_EDGE,
            severity=IssueSeverity.HIGH,
            edge=edge_key,
            description=f"Edge {get_hash_prefix(u)}-{get_hash_prefix(v)} is a single point of failure",
        ))
    
    # Penalize for bridges
    if bridges:
        bridge_penalty = min(0.3, len(bridges) * 0.1)
        redundancy = max(0, redundancy - bridge_penalty)
    
    # Check for articulation points (critical nodes)
    articulation_pts = graph.find_articulation_points()
    for node in articulation_pts:
        issues.append(NetworkIssue(
            type=IssueType.SINGLE_POINT_OF_FAILURE,
            severity=IssueSeverity.HIGH,
            node=node,
            description=f"Node {get_hash_prefix(node)} is critical - its removal would disconnect the network",
        ))
    
    return redundancy, issues


def _calculate_stability_score(conn, hours: int = 168) -> Tuple[float, List[NetworkIssue]]:
    """Calculate stability score based on path consistency."""
    import json
    
    issues = []
    cutoff = time.time() - (hours * 3600)
    cursor = conn.cursor()
    
    # Get path observation patterns
    cursor.execute("""
        SELECT src_hash, dst_hash, path_signature, observation_count, last_seen
        FROM path_observations
        WHERE last_seen > ?
        ORDER BY src_hash, dst_hash, observation_count DESC
    """, (cutoff,))
    
    rows = cursor.fetchall()
    
    if not rows:
        return 0.5, issues  # No data = neutral
    
    # Calculate path stability per source-destination pair
    pair_paths = {}
    for row in rows:
        src = row[0]
        dst = row[1]
        key = f"{src}:{dst}"
        if key not in pair_paths:
            pair_paths[key] = []
        pair_paths[key].append({
            "signature": row[2],
            "count": row[3],
            "last_seen": row[4],
        })
    
    # Stability = percentage of traffic on dominant path
    stability_scores = []
    for key, paths in pair_paths.items():
        if not paths:
            continue
        
        total_obs = sum(p["count"] for p in paths)
        if total_obs == 0:
            continue
        
        dominant_count = paths[0]["count"]  # Already sorted by count DESC
        stability = dominant_count / total_obs
        stability_scores.append(stability)
        
        # Flag high volatility pairs
        if stability < (1 - HIGH_PATH_VOLATILITY_THRESHOLD):
            src, dst = key.split(":", 1)
            issues.append(NetworkIssue(
                type=IssueType.WEAK_LINK,
                severity=IssueSeverity.LOW,
                description=f"Path from {get_hash_prefix(src)} to {get_hash_prefix(dst)} is unstable ({len(paths)} different routes)",
                metric_value=stability,
            ))
    
    if not stability_scores:
        return 0.5, issues
    
    avg_stability = sum(stability_scores) / len(stability_scores)
    return avg_stability, issues


def _calculate_coverage_score(
    conn,
    graph: MeshGraph,
    topology: Optional[TopologySnapshot] = None,
) -> Tuple[float, List[NetworkIssue]]:
    """Calculate coverage score based on signal quality distribution."""
    issues = []
    
    cursor = conn.cursor()
    
    # Get signal quality stats
    cursor.execute("""
        SELECT 
            AVG(rssi) as avg_rssi,
            MIN(rssi) as min_rssi,
            COUNT(DISTINCT pubkey) as node_count,
            SUM(CASE WHEN zero_hop = 1 THEN 1 ELSE 0 END) as zero_hop_count
        FROM adverts
        WHERE rssi IS NOT NULL
    """)
    
    row = cursor.fetchone()
    
    if not row or row[2] == 0:
        return 0.5, issues  # No signal data = neutral
    
    avg_rssi = row[0] or -100
    min_rssi = row[1] or -120
    node_count = row[2]
    zero_hop_count = row[3]
    
    # Coverage score based on signal quality
    # Excellent: > -70 dBm, Good: -70 to -85, Fair: -85 to -100, Poor: < -100
    if avg_rssi > -70:
        signal_score = 1.0
    elif avg_rssi > -85:
        signal_score = 0.7 + (0.3 * (avg_rssi + 85) / 15)
    elif avg_rssi > -100:
        signal_score = 0.4 + (0.3 * (avg_rssi + 100) / 15)
    else:
        signal_score = max(0.1, 0.4 + (0.3 * (avg_rssi + 100) / 20))
    
    # Bonus for zero-hop (direct RF) connections
    if node_count > 0:
        zero_hop_ratio = zero_hop_count / node_count
        signal_score = min(1.0, signal_score + (zero_hop_ratio * 0.2))
    
    # Check for weak edges in topology
    if topology:
        for edge in topology.edges:
            if edge.avg_confidence < WEAK_LINK_CONFIDENCE_THRESHOLD:
                issues.append(NetworkIssue(
                    type=IssueType.WEAK_LINK,
                    severity=IssueSeverity.MEDIUM,
                    edge=edge.edge_key,
                    description=f"Link {get_hash_prefix(edge.from_hash)}-{get_hash_prefix(edge.to_hash)} has low confidence ({edge.avg_confidence:.0%})",
                    metric_value=edge.avg_confidence,
                ))
            
            if edge.symmetry_ratio < ASYMMETRIC_LINK_THRESHOLD:
                issues.append(NetworkIssue(
                    type=IssueType.ASYMMETRIC_LINK,
                    severity=IssueSeverity.LOW,
                    edge=edge.edge_key,
                    description=f"Link {get_hash_prefix(edge.from_hash)}-{get_hash_prefix(edge.to_hash)} is asymmetric ({edge.symmetry_ratio:.0%} symmetry)",
                    metric_value=edge.symmetry_ratio,
                ))
    
    return signal_score, issues


def _detect_overloaded_nodes(
    topology: TopologySnapshot,
    threshold: float = CRITICAL_NODE_TRAFFIC_THRESHOLD,
) -> List[NetworkIssue]:
    """Detect nodes handling disproportionate traffic."""
    issues = []
    
    if not topology.edges:
        return issues
    
    # Calculate traffic per node
    node_traffic = {}
    total_traffic = 0
    
    for edge in topology.edges:
        count = edge.certain_count
        total_traffic += count
        
        node_traffic[edge.from_hash] = node_traffic.get(edge.from_hash, 0) + count
        node_traffic[edge.to_hash] = node_traffic.get(edge.to_hash, 0) + count
    
    if total_traffic == 0:
        return issues
    
    # Check for overloaded nodes
    for node, traffic in node_traffic.items():
        traffic_share = traffic / total_traffic
        if traffic_share > threshold:
            issues.append(NetworkIssue(
                type=IssueType.OVERLOADED_NODE,
                severity=IssueSeverity.MEDIUM if traffic_share < 0.5 else IssueSeverity.HIGH,
                node=node,
                description=f"Node {get_hash_prefix(node)} handles {traffic_share:.0%} of network traffic",
                metric_value=traffic_share,
            ))
    
    return issues


def _generate_recommendations(
    issues: List[NetworkIssue],
    graph: MeshGraph,
    topology: Optional[TopologySnapshot] = None,
) -> List[Recommendation]:
    """Generate actionable recommendations based on detected issues."""
    recommendations = []
    
    # Group issues by type
    issue_by_type = {}
    for issue in issues:
        if issue.type not in issue_by_type:
            issue_by_type[issue.type] = []
        issue_by_type[issue.type].append(issue)
    
    # Single points of failure
    spof_issues = issue_by_type.get(IssueType.SINGLE_POINT_OF_FAILURE, [])
    if spof_issues:
        nodes = [i.node for i in spof_issues if i.node]
        recommendations.append(Recommendation(
            priority=1,
            title="Add Redundant Paths to Critical Nodes",
            description=f"{len(spof_issues)} nodes are single points of failure. Consider adding alternate routes to these nodes to improve network resilience.",
            related_nodes=nodes[:5],  # Top 5
        ))
    
    # Bridge edges
    bridge_issues = issue_by_type.get(IssueType.BRIDGE_EDGE, [])
    if bridge_issues:
        edges = [i.edge for i in bridge_issues if i.edge]
        recommendations.append(Recommendation(
            priority=1,
            title="Add Redundant Links for Critical Edges",
            description=f"{len(bridge_issues)} links are critical connections. Adding alternate paths would prevent network splits if these links fail.",
            related_edges=edges[:5],
        ))
    
    # Weak links
    weak_issues = issue_by_type.get(IssueType.WEAK_LINK, [])
    if len(weak_issues) > 3:
        edges = [i.edge for i in weak_issues if i.edge]
        recommendations.append(Recommendation(
            priority=2,
            title="Investigate Weak Links",
            description=f"{len(weak_issues)} links have low confidence. These may indicate poor RF conditions or intermittent connectivity.",
            related_edges=edges[:5],
        ))
    
    # Overloaded nodes
    overload_issues = issue_by_type.get(IssueType.OVERLOADED_NODE, [])
    if overload_issues:
        nodes = [i.node for i in overload_issues if i.node]
        recommendations.append(Recommendation(
            priority=2,
            title="Distribute Traffic Load",
            description=f"{len(overload_issues)} nodes are handling disproportionate traffic. Consider adding relay nodes to distribute the load.",
            related_nodes=nodes[:3],
        ))
    
    # Isolated nodes
    isolated_issues = issue_by_type.get(IssueType.ISOLATED_NODE, [])
    if isolated_issues:
        nodes = [i.node for i in isolated_issues if i.node]
        recommendations.append(Recommendation(
            priority=3,
            title="Connect Isolated Nodes",
            description=f"{len(isolated_issues)} nodes are isolated from the main network. Verify they are powered on and within RF range.",
            related_nodes=nodes[:5],
        ))
    
    # Low redundancy
    if IssueType.LOW_REDUNDANCY in issue_by_type:
        recommendations.append(Recommendation(
            priority=2,
            title="Improve Network Redundancy",
            description="Network has low path redundancy. Consider adding more mesh connections between existing nodes.",
        ))
    
    # Network fragmentation
    if IssueType.FRAGMENTED_NETWORK in issue_by_type:
        recommendations.append(Recommendation(
            priority=1,
            title="Reconnect Network Segments",
            description="Network is split into multiple disconnected segments. Add relay nodes to bridge the gaps.",
        ))
    
    # Sort by priority
    recommendations.sort(key=lambda r: r.priority)
    
    return recommendations


def get_network_health(
    conn_or_db: DBConnection,
    local_hash: Optional[str] = None,
    min_certainty: int = 5,
    hours: int = 168,
) -> NetworkHealthReport:
    """
    Generate comprehensive network health report.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        local_hash: Local node's hash
        min_certainty: Minimum certainty for graph edges
        hours: Time range for stability analysis
        
    Returns:
        NetworkHealthReport with scores, issues, and recommendations
    """
    start_time = time.time()
    
    report = NetworkHealthReport(
        health_score=0.0,
        metrics=HealthMetrics(),
    )
    
    if not NETWORKX_AVAILABLE:
        logger.warning("NetworkX not available, limited health analysis")
        report.analysis_duration_ms = (time.time() - start_time) * 1000
        return report
    
    with ensure_connection(conn_or_db) as conn:
        all_issues = []
        
        # Build graph
        try:
            graph = build_graph_from_db(conn, min_certainty, local_hash)
            report.total_nodes = graph.node_count
            report.total_edges = graph.edge_count // 2  # Undirected count
        except Exception as e:
            logger.error(f"Failed to build graph: {e}")
            report.analysis_duration_ms = (time.time() - start_time) * 1000
            return report
        
        # Build topology for additional analysis
        try:
            topology = build_topology_from_db(conn, local_hash, min_certainty)
            report.hub_count = len(topology.hub_nodes)
        except Exception as e:
            logger.warning(f"Failed to build topology: {e}")
            topology = None
        
        # Calculate connectivity score
        connectivity, conn_issues = _calculate_connectivity_score(graph)
        report.metrics.connectivity = connectivity
        all_issues.extend(conn_issues)
        
        # Calculate redundancy score
        redundancy, red_issues = _calculate_redundancy_score(graph)
        report.metrics.redundancy = redundancy
        all_issues.extend(red_issues)
        
        # Count critical infrastructure
        report.articulation_point_count = len(graph.find_articulation_points())
        report.bridge_count = len(graph.find_bridges())
        
        # Get component count
        components = graph.connected_components()
        report.component_count = components.total_components
        
        # Calculate stability score
        stability, stab_issues = _calculate_stability_score(conn, hours)
        report.metrics.stability = stability
        all_issues.extend(stab_issues)
        
        # Calculate coverage score
        coverage, cov_issues = _calculate_coverage_score(conn, graph, topology)
        report.metrics.coverage = coverage
        all_issues.extend(cov_issues)
        
        # Detect overloaded nodes
        if topology:
            overload_issues = _detect_overloaded_nodes(topology)
            all_issues.extend(overload_issues)
        
        # Calculate overall health score
        report.health_score = (
            report.metrics.connectivity * 0.25 +
            report.metrics.redundancy * 0.25 +
            report.metrics.stability * 0.25 +
            report.metrics.coverage * 0.25
        )
        
        # Penalize for critical issues
        critical_count = sum(1 for i in all_issues if i.severity == IssueSeverity.CRITICAL)
        high_count = sum(1 for i in all_issues if i.severity == IssueSeverity.HIGH)
        
        penalty = (critical_count * 0.1) + (high_count * 0.03)
        report.health_score = max(0, report.health_score - penalty)
        
        # Sort issues by severity
        severity_order = {
            IssueSeverity.CRITICAL: 0,
            IssueSeverity.HIGH: 1,
            IssueSeverity.MEDIUM: 2,
            IssueSeverity.LOW: 3,
            IssueSeverity.INFO: 4,
        }
        all_issues.sort(key=lambda i: severity_order.get(i.severity, 5))
        report.issues = all_issues
        
        # Generate recommendations
        report.recommendations = _generate_recommendations(all_issues, graph, topology)
    
    report.analysis_duration_ms = (time.time() - start_time) * 1000
    return report


def get_quick_health_score(
    conn_or_db: DBConnection,
    local_hash: Optional[str] = None,
    min_certainty: int = 5,
) -> float:
    """
    Get just the overall health score without full analysis.
    
    Faster than full get_network_health() for dashboard display.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        local_hash: Local node's hash
        min_certainty: Minimum certainty for graph edges
        
    Returns:
        Health score from 0 to 1
    """
    if not NETWORKX_AVAILABLE:
        return 0.5
    
    with ensure_connection(conn_or_db) as conn:
        try:
            graph = build_graph_from_db(conn, min_certainty, local_hash)
            
            # Quick connectivity check
            if not graph.is_connected():
                return 0.3
            
            # Use resilience score as quick health proxy
            resilience = graph.resilience_score()
            
            # Adjust based on efficiency
            efficiency = graph.global_efficiency()
            
            return (resilience * 0.6) + (efficiency * 0.4)
            
        except Exception as e:
            logger.error(f"Failed to calculate quick health: {e}")
            return 0.5
