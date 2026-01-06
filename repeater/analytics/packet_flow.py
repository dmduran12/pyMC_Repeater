"""
Packet Flow Analysis - Traffic flow data for visualization
==========================================================

Aggregates packet traffic between nodes to generate flow data
suitable for Sankey diagram visualization. Tracks source-to-destination
flows through intermediate relay nodes.

Key Concepts
------------
    Flow:
        A directed traffic stream from one node to another.
        Measured in packet count and total bytes.
        
    Sankey Data:
        Format suitable for Sankey diagrams with:
        - nodes: List of unique node identifiers
        - links: List of {source, target, value} objects
        
    Flow Aggregation:
        Combines individual packet observations into aggregate
        flows over the specified time period.

Public Functions
----------------
    get_packet_flows(conn, hours, min_packets)
        Get aggregated packet flows for Sankey visualization.
        
    get_flow_matrix(conn, hours)
        Get source-destination flow matrix.
        
    get_top_talkers(conn, hours, limit)
        Get nodes with highest traffic volume.

See Also
--------
    - edge_builder.py: Uses similar path analysis
    - topology.py: Network structure context
"""

import logging
import sqlite3
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set

from .db import with_connection, DBConnection

logger = logging.getLogger("Analytics.PacketFlow")

# Flow analysis constants
MIN_FLOW_PACKETS = 5  # Minimum packets to include a flow


@dataclass
class FlowNode:
    """Node in the flow graph."""
    hash: str
    prefix: str
    name: Optional[str]
    is_local: bool
    total_sent: int
    total_received: int
    total_forwarded: int
    
    def to_dict(self) -> dict:
        return {
            "hash": self.hash,
            "prefix": self.prefix,
            "name": self.name,
            "isLocal": self.is_local,
            "totalSent": self.total_sent,
            "totalReceived": self.total_received,
            "totalForwarded": self.total_forwarded,
        }


@dataclass
class FlowLink:
    """Directed flow link between two nodes."""
    source_hash: str
    target_hash: str
    packet_count: int
    byte_count: int
    avg_rssi: Optional[float]
    avg_snr: Optional[float]
    
    def to_dict(self) -> dict:
        return {
            "source": self.source_hash,
            "target": self.target_hash,
            "value": self.packet_count,
            "packetCount": self.packet_count,
            "byteCount": self.byte_count,
            "avgRssi": round(self.avg_rssi, 1) if self.avg_rssi else None,
            "avgSnr": round(self.avg_snr, 1) if self.avg_snr else None,
        }


@dataclass
class SankeyData:
    """Data formatted for Sankey diagram visualization."""
    nodes: List[FlowNode]
    links: List[FlowLink]
    total_packets: int
    total_bytes: int
    time_range_hours: int
    
    def to_dict(self) -> dict:
        # For Sankey, we need node indices
        node_indices = {n.hash: i for i, n in enumerate(self.nodes)}
        
        return {
            "nodes": [n.to_dict() for n in self.nodes],
            "links": [
                {
                    "source": node_indices.get(l.source_hash, 0),
                    "target": node_indices.get(l.target_hash, 0),
                    "value": l.packet_count,
                    "sourceHash": l.source_hash,
                    "targetHash": l.target_hash,
                    "packetCount": l.packet_count,
                    "byteCount": l.byte_count,
                    "avgRssi": round(l.avg_rssi, 1) if l.avg_rssi else None,
                    "avgSnr": round(l.avg_snr, 1) if l.avg_snr else None,
                }
                for l in self.links
                if l.source_hash in node_indices and l.target_hash in node_indices
            ],
            "totalPackets": self.total_packets,
            "totalBytes": self.total_bytes,
            "timeRangeHours": self.time_range_hours,
        }


@dataclass
class TopTalker:
    """Node with high traffic volume."""
    hash: str
    prefix: str
    name: Optional[str]
    packets_sent: int
    packets_received: int
    packets_forwarded: int
    bytes_sent: int
    bytes_received: int
    unique_destinations: int
    unique_sources: int
    
    def to_dict(self) -> dict:
        return {
            "hash": self.hash,
            "prefix": self.prefix,
            "name": self.name,
            "packetsSent": self.packets_sent,
            "packetsReceived": self.packets_received,
            "packetsForwarded": self.packets_forwarded,
            "bytesSent": self.bytes_sent,
            "bytesReceived": self.bytes_received,
            "uniqueDestinations": self.unique_destinations,
            "uniqueSources": self.unique_sources,
            "totalTraffic": self.packets_sent + self.packets_received + self.packets_forwarded,
        }


@dataclass
class FlowMatrix:
    """Source-destination flow matrix."""
    nodes: List[str]  # Ordered list of node hashes
    matrix: List[List[int]]  # matrix[src_idx][dst_idx] = packet count
    node_names: Dict[str, Optional[str]]
    
    def to_dict(self) -> dict:
        return {
            "nodes": self.nodes,
            "matrix": self.matrix,
            "nodeNames": self.node_names,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# Core Analysis Functions
# ═══════════════════════════════════════════════════════════════════════════════

def _get_node_info(conn: DBConnection) -> Dict[str, Tuple[str, Optional[str]]]:
    """Get node prefix and name lookup."""
    cursor = conn.execute("""
        SELECT pubkey, node_name
        FROM adverts
        GROUP BY pubkey
        HAVING MAX(last_seen)
    """)
    
    result = {}
    for row in cursor:
        pubkey = row["pubkey"]
        prefix = pubkey[2:4] if pubkey.startswith("0x") else pubkey[:2]
        result[pubkey] = (prefix.upper(), row["node_name"])
    
    return result


def _extract_flows_from_packets(
    conn: DBConnection,
    hours: int,
    local_hash: Optional[str] = None,
) -> Tuple[Dict[str, Dict[str, Dict]], Dict[str, Dict[str, int]], int, int]:
    """
    Extract flow data from packet records.
    
    Returns:
        Tuple of (flows dict, node stats, total packets, total bytes)
        
    flows[src][dst] = {packets, bytes, rssi_sum, snr_sum, samples}
    node_stats[hash] = {sent, received, forwarded, bytes_sent, bytes_received}
    """
    cutoff = time.time() - (hours * 3600)
    
    # Query packets with source and destination
    cursor = conn.execute("""
        SELECT 
            src_hash,
            dst_hash,
            length,
            rssi,
            snr,
            transmitted,
            original_path
        FROM packets
        WHERE timestamp > ?
        AND src_hash IS NOT NULL
    """, (cutoff,))
    
    flows = defaultdict(lambda: defaultdict(lambda: {
        "packets": 0,
        "bytes": 0,
        "rssi_sum": 0.0,
        "snr_sum": 0.0,
        "samples": 0,
    }))
    
    node_stats = defaultdict(lambda: {
        "sent": 0,
        "received": 0,
        "forwarded": 0,
        "bytes_sent": 0,
        "bytes_received": 0,
        "destinations": set(),
        "sources": set(),
    })
    
    total_packets = 0
    total_bytes = 0
    
    for row in cursor:
        src = row["src_hash"]
        dst = row["dst_hash"]
        length = row["length"] or 0
        rssi = row["rssi"]
        snr = row["snr"]
        transmitted = row["transmitted"]
        
        if not src:
            continue
            
        total_packets += 1
        total_bytes += length
        
        # Track sender
        node_stats[src]["sent"] += 1
        node_stats[src]["bytes_sent"] += length
        if dst:
            node_stats[src]["destinations"].add(dst)
        
        # Track receiver
        if dst:
            node_stats[dst]["received"] += 1
            node_stats[dst]["bytes_received"] += length
            node_stats[dst]["sources"].add(src)
            
            # Add to flow
            flow = flows[src][dst]
            flow["packets"] += 1
            flow["bytes"] += length
            if rssi is not None:
                flow["rssi_sum"] += rssi
                flow["samples"] += 1
            if snr is not None:
                flow["snr_sum"] += snr
        
        # Track forwarding from path
        if transmitted and local_hash:
            # If we transmitted and we're not the source, we forwarded
            if src != local_hash:
                node_stats[local_hash]["forwarded"] += 1
    
    return dict(flows), dict(node_stats), total_packets, total_bytes


def _build_flow_links(
    flows: Dict[str, Dict[str, Dict]],
    min_packets: int,
) -> List[FlowLink]:
    """Build FlowLink objects from raw flow data."""
    links = []
    
    for src, destinations in flows.items():
        for dst, data in destinations.items():
            if data["packets"] >= min_packets:
                avg_rssi = None
                avg_snr = None
                if data["samples"] > 0:
                    avg_rssi = data["rssi_sum"] / data["samples"]
                    avg_snr = data["snr_sum"] / data["samples"]
                
                links.append(FlowLink(
                    source_hash=src,
                    target_hash=dst,
                    packet_count=data["packets"],
                    byte_count=data["bytes"],
                    avg_rssi=avg_rssi,
                    avg_snr=avg_snr,
                ))
    
    # Sort by packet count descending
    links.sort(key=lambda l: l.packet_count, reverse=True)
    return links


def _build_flow_nodes(
    node_stats: Dict[str, Dict],
    node_info: Dict[str, Tuple[str, Optional[str]]],
    links: List[FlowLink],
    local_hash: Optional[str] = None,
) -> List[FlowNode]:
    """Build FlowNode objects for nodes involved in flows."""
    # Get set of nodes involved in links
    involved_nodes = set()
    for link in links:
        involved_nodes.add(link.source_hash)
        involved_nodes.add(link.target_hash)
    
    nodes = []
    for node_hash in involved_nodes:
        info = node_info.get(node_hash, (node_hash[:2].upper(), None))
        stats = node_stats.get(node_hash, {
            "sent": 0, "received": 0, "forwarded": 0
        })
        
        nodes.append(FlowNode(
            hash=node_hash,
            prefix=info[0],
            name=info[1],
            is_local=(node_hash == local_hash),
            total_sent=stats.get("sent", 0),
            total_received=stats.get("received", 0),
            total_forwarded=stats.get("forwarded", 0),
        ))
    
    # Sort by total traffic
    nodes.sort(key=lambda n: n.total_sent + n.total_received, reverse=True)
    return nodes


# ═══════════════════════════════════════════════════════════════════════════════
# Main API Functions
# ═══════════════════════════════════════════════════════════════════════════════

@with_connection
def get_packet_flows(
    conn: DBConnection,
    hours: int = 24,
    min_packets: int = MIN_FLOW_PACKETS,
    local_hash: Optional[str] = None,
    max_links: int = 100,
) -> SankeyData:
    """
    Get aggregated packet flows for Sankey visualization.
    
    Args:
        conn: Database connection
        hours: Time range in hours (default: 24)
        min_packets: Minimum packets for a flow to be included
        local_hash: Local node hash for context
        max_links: Maximum number of links to return
        
    Returns:
        SankeyData with nodes and links for visualization
    """
    # Get node info for names
    node_info = _get_node_info(conn)
    
    # Extract flows from packets
    flows, node_stats, total_packets, total_bytes = _extract_flows_from_packets(
        conn, hours, local_hash
    )
    
    # Build links
    links = _build_flow_links(flows, min_packets)
    
    # Limit links
    if len(links) > max_links:
        links = links[:max_links]
    
    # Build nodes for the links we have
    nodes = _build_flow_nodes(node_stats, node_info, links, local_hash)
    
    return SankeyData(
        nodes=nodes,
        links=links,
        total_packets=total_packets,
        total_bytes=total_bytes,
        time_range_hours=hours,
    )


@with_connection
def get_flow_matrix(
    conn: DBConnection,
    hours: int = 24,
    min_packets: int = 1,
    limit_nodes: int = 20,
) -> FlowMatrix:
    """
    Get source-destination flow matrix.
    
    Args:
        conn: Database connection
        hours: Time range in hours
        min_packets: Minimum packets for inclusion
        limit_nodes: Maximum nodes to include (top by traffic)
        
    Returns:
        FlowMatrix with nodes and packet counts
    """
    # Get node info
    node_info = _get_node_info(conn)
    
    # Extract flows
    flows, node_stats, _, _ = _extract_flows_from_packets(conn, hours)
    
    # Get top nodes by traffic
    node_traffic = []
    for node_hash, stats in node_stats.items():
        total = stats.get("sent", 0) + stats.get("received", 0)
        node_traffic.append((node_hash, total))
    
    node_traffic.sort(key=lambda x: x[1], reverse=True)
    top_nodes = [n[0] for n in node_traffic[:limit_nodes]]
    
    # Build matrix
    n = len(top_nodes)
    matrix = [[0] * n for _ in range(n)]
    node_indices = {h: i for i, h in enumerate(top_nodes)}
    
    for src, destinations in flows.items():
        if src not in node_indices:
            continue
        src_idx = node_indices[src]
        
        for dst, data in destinations.items():
            if dst not in node_indices:
                continue
            if data["packets"] >= min_packets:
                dst_idx = node_indices[dst]
                matrix[src_idx][dst_idx] = data["packets"]
    
    # Build node names dict
    node_names = {}
    for node_hash in top_nodes:
        info = node_info.get(node_hash)
        node_names[node_hash] = info[1] if info else None
    
    return FlowMatrix(
        nodes=top_nodes,
        matrix=matrix,
        node_names=node_names,
    )


@with_connection
def get_top_talkers(
    conn: DBConnection,
    hours: int = 24,
    limit: int = 10,
) -> List[TopTalker]:
    """
    Get nodes with highest traffic volume.
    
    Args:
        conn: Database connection
        hours: Time range in hours
        limit: Number of top talkers to return
        
    Returns:
        List of TopTalker sorted by total traffic
    """
    # Get node info
    node_info = _get_node_info(conn)
    
    # Extract flows
    _, node_stats, _, _ = _extract_flows_from_packets(conn, hours)
    
    # Build top talkers
    talkers = []
    for node_hash, stats in node_stats.items():
        info = node_info.get(node_hash, (node_hash[:2].upper(), None))
        
        talkers.append(TopTalker(
            hash=node_hash,
            prefix=info[0],
            name=info[1],
            packets_sent=stats.get("sent", 0),
            packets_received=stats.get("received", 0),
            packets_forwarded=stats.get("forwarded", 0),
            bytes_sent=stats.get("bytes_sent", 0),
            bytes_received=stats.get("bytes_received", 0),
            unique_destinations=len(stats.get("destinations", set())),
            unique_sources=len(stats.get("sources", set())),
        ))
    
    # Sort by total traffic
    talkers.sort(
        key=lambda t: t.packets_sent + t.packets_received + t.packets_forwarded,
        reverse=True
    )
    
    return talkers[:limit]


@with_connection
def get_flow_summary(
    conn: DBConnection,
    hours: int = 24,
) -> dict:
    """
    Get quick summary of packet flows.
    
    Lighter-weight alternative to full flow analysis.
    """
    cutoff = time.time() - (hours * 3600)
    
    # Quick packet stats
    cursor = conn.execute("""
        SELECT 
            COUNT(*) as total_packets,
            SUM(length) as total_bytes,
            COUNT(DISTINCT src_hash) as unique_sources,
            COUNT(DISTINCT dst_hash) as unique_destinations
        FROM packets
        WHERE timestamp > ?
        AND src_hash IS NOT NULL
    """, (cutoff,))
    
    row = cursor.fetchone()
    
    return {
        "totalPackets": row["total_packets"] or 0,
        "totalBytes": row["total_bytes"] or 0,
        "uniqueSources": row["unique_sources"] or 0,
        "uniqueDestinations": row["unique_destinations"] or 0,
        "timeRangeHours": hours,
    }
