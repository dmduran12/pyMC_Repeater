"""
Bucketing - Time-series aggregation for airtime charts
=======================================================

Aggregates packet data into fixed-width time buckets for visualization.
Supports 21 preset time ranges from 20 minutes to 10 years, with
automatically calculated bucket sizes for optimal chart granularity.

This module powers the airtime utilization charts in the dashboard,
showing RF activity over time broken down by packet type.

Key Concepts
------------
    Bucket:
        Fixed time window containing aggregated metrics (packet counts,
        airtime, signal stats). Bucket width varies by time range to
        maintain ~300 data points for smooth chart rendering.
        
    Airtime:
        Estimated RF transmission time in milliseconds, calculated using
        the Semtech LoRa formula based on packet size, spreading factor,
        bandwidth, and coding rate.
        
    Utilization:
        Percentage of bucket time spent transmitting/receiving. Useful
        for detecting channel congestion and duty cycle compliance.
        
    Preset:
        Named time range with optimized bucket size. Use presets for
        consistent behavior across the application.

Time Range Presets
------------------
    Short-term (high granularity):
        20m  - 20 minutes  @ 10s buckets  (120 points)
        1h   - 1 hour      @ 10s buckets  (360 points)
        3h   - 3 hours     @ 30s buckets  (360 points)
        
    Medium-term:
        12h  - 12 hours    @ 2min buckets  (360 points)
        1d   - 1 day       @ 5min buckets  (288 points)
        2d-7d - 2-7 days   @ 10-30min      (288-336 points)
        
    Long-term (lower granularity):
        14d  - 14 days     @ 1hr buckets   (336 points)
        30d  - 30 days     @ 2hr buckets   (360 points)
        3mo  - 3 months    @ 6hr buckets   (360 points)
        6mo  - 6 months    @ 12hr buckets  (360 points)
        
    Historical (archival):
        1yr-10yr - 1-10 years @ 1-10 day buckets (365 points)

Airtime Calculation
-------------------
Uses the Semtech LoRa airtime formula:

    T_packet = T_preamble + T_payload
    
Where:
    T_preamble = (n_preamble + 4.25) * T_symbol
    T_symbol = 2^SF / BW
    T_payload = n_payload_symbols * T_symbol

This matches the calculation in pymc_core's PacketTimingUtils.

Public Functions
----------------
    get_airtime_utilization_preset(conn, preset, radio_config)
        Query bucketed stats using a preset time range. RECOMMENDED.
        
    get_airtime_utilization(conn, minutes, bucket_count, radio_config)
        Query with custom time range. Legacy support.
        
    get_available_presets()
        Get list of all presets for UI dropdown population.
        
    parse_time_range(range_str)
        Parse preset string to (total_seconds, bucket_seconds, label).

API Usage
---------
    # Using preset (recommended)
    GET /api/analytics/bucketed_stats?preset=7d
    
    # Get available presets for dropdown
    GET /api/analytics/time_presets
    
    # Legacy support (still works)
    GET /api/analytics/bucketed_stats?minutes=60&bucket_count=360

Response Format
---------------
    {
        "success": true,
        "data": {
            "preset": "7d",
            "preset_label": "7 Days",
            "time_range_minutes": 10080,
            "bucket_duration_seconds": 1800,
            "start_time": 1704412800,
            "end_time": 1705017600,
            "received": [{"bucket": ..., "count": ..., "airtime_ms": ...}],
            "transmitted": [...],
            "forwarded": [...],
            "dropped": [...]
        }
    }

See Also
--------
    - analytics_api.py: REST endpoints for bucketed stats
    - pymc_core.protocol.packet_utils.PacketTimingUtils: Airtime calc
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Union
import sqlite3

from .db import AnalyticsDB, ensure_connection, DBConnection

logger = logging.getLogger("Analytics.Bucketing")

# Default bucket duration for airtime charts (10 seconds)
DEFAULT_BUCKET_SECONDS = 10

# Time constants
MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR
WEEK = 7 * DAY
MONTH = 30 * DAY  # Approximate
YEAR = 365 * DAY  # Approximate

# Time range presets: (total_seconds, bucket_seconds, label)
# Bucket sizes are chosen for reasonable granularity (~50-500 buckets)
TIME_RANGE_PRESETS = {
    # Short-term (high granularity)
    "20m": (20 * MINUTE, 10, "20 Minutes"),           # 120 buckets @ 10s
    "1h": (1 * HOUR, 10, "1 Hour"),                   # 360 buckets @ 10s
    "3h": (3 * HOUR, 30, "3 Hours"),                  # 360 buckets @ 30s
    
    # Medium-term
    "12h": (12 * HOUR, 2 * MINUTE, "12 Hours"),       # 360 buckets @ 2min
    "1d": (1 * DAY, 5 * MINUTE, "1 Day"),             # 288 buckets @ 5min
    "2d": (2 * DAY, 10 * MINUTE, "2 Days"),           # 288 buckets @ 10min
    "3d": (3 * DAY, 15 * MINUTE, "3 Days"),           # 288 buckets @ 15min
    "4d": (4 * DAY, 20 * MINUTE, "4 Days"),           # 288 buckets @ 20min
    "5d": (5 * DAY, 25 * MINUTE, "5 Days"),           # 288 buckets @ 25min
    "6d": (6 * DAY, 30 * MINUTE, "6 Days"),           # 288 buckets @ 30min
    "7d": (7 * DAY, 30 * MINUTE, "7 Days"),           # 336 buckets @ 30min
    
    # Long-term (lower granularity)
    "14d": (14 * DAY, 1 * HOUR, "14 Days"),           # 336 buckets @ 1hr
    "30d": (30 * DAY, 2 * HOUR, "30 Days"),           # 360 buckets @ 2hr
    "3mo": (3 * MONTH, 6 * HOUR, "3 Months"),         # 360 buckets @ 6hr
    "6mo": (6 * MONTH, 12 * HOUR, "6 Months"),        # 360 buckets @ 12hr
    
    # Historical (archival)
    "1yr": (1 * YEAR, 1 * DAY, "1 Year"),             # 365 buckets @ 1day
    "2yr": (2 * YEAR, 2 * DAY, "2 Years"),            # 365 buckets @ 2days
    "3yr": (3 * YEAR, 3 * DAY, "3 Years"),            # 365 buckets @ 3days
    "4yr": (4 * YEAR, 4 * DAY, "4 Years"),            # 365 buckets @ 4days
    "5yr": (5 * YEAR, 5 * DAY, "5 Years"),            # 365 buckets @ 5days
    "10yr": (10 * YEAR, 10 * DAY, "10 Years"),        # 365 buckets @ 10days
}

# Ordered list for UI dropdown
TIME_RANGE_OPTIONS = [
    "20m", "1h", "3h", "12h",
    "1d", "2d", "3d", "4d", "5d", "6d", "7d",
    "14d", "30d", "3mo", "6mo",
    "1yr", "2yr", "3yr", "4yr", "5yr", "10yr",
]

# LoRa airtime calculation constants (Semtech formula)
LORA_PREAMBLE_SYMBOLS = 8
LORA_EXPLICIT_HEADER = True
LORA_CRC_ON = True
LORA_LOW_DATA_RATE_OPTIMIZE_AUTO = True


@dataclass
class UtilBucket:
    """Single bucket of utilization data."""
    bucket_start: int  # Unix timestamp (seconds)
    bucket_end: int
    rx_count: int = 0
    tx_count: int = 0
    forward_count: int = 0
    drop_count: int = 0
    airtime_ms: float = 0.0
    avg_snr: Optional[float] = None
    avg_rssi: Optional[float] = None
    
    # Internal accumulators
    _snr_sum: float = field(default=0.0, repr=False)
    _rssi_sum: float = field(default=0.0, repr=False)
    _signal_count: int = field(default=0, repr=False)
    
    def add_packet(
        self,
        airtime_ms: float,
        is_tx: bool,
        is_forward: bool,
        is_dropped: bool,
        snr: Optional[float] = None,
        rssi: Optional[float] = None,
    ):
        """Add a packet to this bucket."""
        self.airtime_ms += airtime_ms
        
        if is_tx:
            self.tx_count += 1
        else:
            self.rx_count += 1
        
        if is_forward:
            self.forward_count += 1
        if is_dropped:
            self.drop_count += 1
        
        # Track signal stats
        if snr is not None:
            self._snr_sum += snr
            self._signal_count += 1
        if rssi is not None:
            self._rssi_sum += rssi
    
    def finalize(self):
        """Compute averages after all packets added."""
        if self._signal_count > 0:
            self.avg_snr = round(self._snr_sum / self._signal_count, 2)
            self.avg_rssi = round(self._rssi_sum / self._signal_count, 1)
    
    def to_dict(self) -> dict:
        """Convert to API response format."""
        return {
            "bucket": self.bucket_start,
            "start": self.bucket_start,
            "end": self.bucket_end,
            "count": self.rx_count + self.tx_count,
            "rx_count": self.rx_count,
            "tx_count": self.tx_count,
            "forward_count": self.forward_count,
            "drop_count": self.drop_count,
            "airtime_ms": round(self.airtime_ms, 2),
            "avg_snr": self.avg_snr,
            "avg_rssi": self.avg_rssi,
        }


@dataclass
class BucketedStats:
    """Complete bucketed statistics response."""
    time_range_minutes: int
    bucket_count: int
    bucket_duration_seconds: int
    start_time: int
    end_time: int
    preset: Optional[str] = None  # The preset key used (e.g. "1h", "7d")
    preset_label: Optional[str] = None  # Human-readable label
    received: List[UtilBucket] = field(default_factory=list)
    transmitted: List[UtilBucket] = field(default_factory=list)
    forwarded: List[UtilBucket] = field(default_factory=list)
    dropped: List[UtilBucket] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        """Convert to API response format."""
        result = {
            "time_range_minutes": self.time_range_minutes,
            "bucket_count": self.bucket_count,
            "bucket_duration_seconds": self.bucket_duration_seconds,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "received": [b.to_dict() for b in self.received],
            "transmitted": [b.to_dict() for b in self.transmitted],
            "forwarded": [b.to_dict() for b in self.forwarded],
            "dropped": [b.to_dict() for b in self.dropped],
        }
        if self.preset:
            result["preset"] = self.preset
            result["preset_label"] = self.preset_label
        return result


def compute_time_bucket(timestamp: float, bucket_seconds: int) -> int:
    """
    Compute bucket start timestamp for a given time.
    
    Args:
        timestamp: Unix timestamp (seconds, may be float)
        bucket_seconds: Bucket width in seconds
        
    Returns:
        Bucket start timestamp (aligned to bucket boundary)
    """
    ts = int(timestamp)
    return (ts // bucket_seconds) * bucket_seconds


def estimate_lora_airtime_ms(
    payload_length: int,
    spreading_factor: int = 8,
    bandwidth_hz: int = 125000,
    coding_rate: int = 5,
    preamble_length: int = 8,
) -> float:
    """
    Estimate LoRa packet airtime using Semtech formula.
    
    This matches the calculation in pymc_core's PacketTimingUtils.
    
    Args:
        payload_length: Payload size in bytes
        spreading_factor: LoRa SF (7-12)
        bandwidth_hz: Bandwidth in Hz
        coding_rate: Coding rate denominator (5-8 for 4/5 to 4/8)
        preamble_length: Number of preamble symbols
        
    Returns:
        Estimated airtime in milliseconds
    """
    # Symbol duration: T_sym = 2^SF / BW
    symbol_duration_ms = (2 ** spreading_factor) / bandwidth_hz * 1000
    
    # Preamble duration
    preamble_duration_ms = (preamble_length + 4.25) * symbol_duration_ms
    
    # Low data rate optimization
    low_data_rate_opt = 1 if (spreading_factor >= 11 and bandwidth_hz == 125000) else 0
    
    # Payload symbol count (Semtech formula)
    # n_payload = 8 + max(ceil((8*PL - 4*SF + 28 + 16*CRC - 20*H) / (4*(SF-2*DE))) * (CR+4), 0)
    h = 0 if LORA_EXPLICIT_HEADER else 1
    crc = 1 if LORA_CRC_ON else 0
    
    numerator = 8 * payload_length - 4 * spreading_factor + 28 + 16 * crc - 20 * h
    denominator = 4 * (spreading_factor - 2 * low_data_rate_opt)
    
    if denominator <= 0:
        denominator = 1
    
    import math
    payload_symbols = 8 + max(math.ceil(numerator / denominator) * coding_rate, 0)
    
    # Payload duration
    payload_duration_ms = payload_symbols * symbol_duration_ms
    
    return preamble_duration_ms + payload_duration_ms


def aggregate_packet_buckets(
    conn_or_db: DBConnection,
    start_ts: int,
    end_ts: int,
    bucket_seconds: int = DEFAULT_BUCKET_SECONDS,
    radio_config: Optional[dict] = None,
) -> BucketedStats:
    """
    Aggregate packets into time buckets from database.
    
    Note: This function queries the 'packets' table which is created by
    the main SQLiteHandler, not by the analytics module. If the packets
    table doesn't exist yet, empty bucket stats are returned.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        start_ts: Start timestamp (seconds)
        end_ts: End timestamp (seconds)
        bucket_seconds: Bucket width in seconds
        radio_config: Radio config for airtime calculation
        
    Returns:
        BucketedStats with all bucket arrays (empty if no packets table)
    """
    # Default radio config for airtime calculation
    sf = radio_config.get("spreading_factor", 8) if radio_config else 8
    bw = radio_config.get("bandwidth", 125000) if radio_config else 125000
    cr = radio_config.get("coding_rate", 5) if radio_config else 5
    
    # Calculate bucket count and duration
    time_range_seconds = end_ts - start_ts
    bucket_count = max(1, time_range_seconds // bucket_seconds)
    time_range_minutes = time_range_seconds // 60
    
    # Initialize bucket arrays
    received = {}
    transmitted = {}
    forwarded = {}
    dropped = {}
    
    def get_or_create_bucket(bucket_dict: dict, bucket_start: int) -> UtilBucket:
        if bucket_start not in bucket_dict:
            bucket_dict[bucket_start] = UtilBucket(
                bucket_start=bucket_start,
                bucket_end=bucket_start + bucket_seconds,
            )
        return bucket_dict[bucket_start]
    
    with ensure_connection(conn_or_db) as conn:
        cursor = conn.cursor()
        
        # Query packets in time range
        # Note: 'packets' table is created by SQLiteHandler, not analytics
        try:
            cursor.execute("""
                SELECT 
                    timestamp, type, route, length, rssi, snr,
                    transmitted, is_duplicate, drop_reason, payload_length
                FROM packets
                WHERE timestamp >= ? AND timestamp < ?
                ORDER BY timestamp
            """, (start_ts, end_ts))
        except Exception as e:
            # packets table may not exist if SQLiteHandler hasn't run yet
            if "no such table: packets" in str(e):
                logger.warning("packets table not found - returning empty stats")
                return BucketedStats(
                    time_range_minutes=time_range_minutes,
                    bucket_count=bucket_count,
                    bucket_duration_seconds=bucket_seconds,
                    start_time=start_ts,
                    end_time=end_ts,
                    received=[],
                    transmitted=[],
                    forwarded=[],
                    dropped=[],
                )
            raise  # Re-raise unexpected errors
        
        for row in cursor.fetchall():
            ts = row[0]
            pkt_type = row[1]
            route = row[2]
            length = row[3] or 0
            rssi = row[4]
            snr = row[5]
            was_transmitted = bool(row[6])
            is_duplicate = bool(row[7])
            drop_reason = row[8]
            payload_length = row[9] or length
            
            bucket_start = compute_time_bucket(ts, bucket_seconds)
            
            # Skip if outside range (edge case)
            if bucket_start < start_ts or bucket_start >= end_ts:
                continue
            
            # Estimate airtime
            airtime_ms = estimate_lora_airtime_ms(
                payload_length=payload_length,
                spreading_factor=sf,
                bandwidth_hz=bw,
                coding_rate=cr,
            )
            
            is_tx = was_transmitted
            is_forward = was_transmitted and not is_duplicate
            is_dropped = drop_reason is not None
            
            # Categorize into buckets
            # Received: all non-local packets
            if not is_tx:
                bucket = get_or_create_bucket(received, bucket_start)
                bucket.add_packet(airtime_ms, False, False, is_dropped, snr, rssi)
            
            # Transmitted: packets we originated or forwarded
            if is_tx:
                bucket = get_or_create_bucket(transmitted, bucket_start)
                bucket.add_packet(airtime_ms, True, is_forward, False, snr, rssi)
            
            # Forwarded: subset of transmitted
            if is_forward:
                bucket = get_or_create_bucket(forwarded, bucket_start)
                bucket.add_packet(airtime_ms, True, True, False, snr, rssi)
            
            # Dropped
            if is_dropped:
                bucket = get_or_create_bucket(dropped, bucket_start)
                bucket.add_packet(airtime_ms, False, False, True, snr, rssi)
    
    # Finalize and sort buckets
    def finalize_buckets(bucket_dict: dict) -> List[UtilBucket]:
        buckets = list(bucket_dict.values())
        for b in buckets:
            b.finalize()
        return sorted(buckets, key=lambda x: x.bucket_start)
    
    return BucketedStats(
        time_range_minutes=time_range_minutes,
        bucket_count=bucket_count,
        bucket_duration_seconds=bucket_seconds,
        start_time=start_ts,
        end_time=end_ts,
        received=finalize_buckets(received),
        transmitted=finalize_buckets(transmitted),
        forwarded=finalize_buckets(forwarded),
        dropped=finalize_buckets(dropped),
    )


def parse_time_range(range_str: str) -> Tuple[int, int, str]:
    """
    Parse a time range string into (total_seconds, bucket_seconds, label).
    
    Supports preset strings like "1h", "7d", "3mo" or custom formats.
    
    Args:
        range_str: Time range preset string
        
    Returns:
        Tuple of (total_seconds, bucket_seconds, human_label)
        
    Raises:
        ValueError: If range string is invalid
    """
    range_str = range_str.lower().strip()
    
    # Check presets first
    if range_str in TIME_RANGE_PRESETS:
        return TIME_RANGE_PRESETS[range_str]
    
    # Try to parse custom format: "<number><unit>"
    # Supported units: m (minutes), h (hours), d (days), w (weeks), mo (months), y (years)
    import re
    match = re.match(r"^(\d+)(m|h|d|w|mo|y|yr)$", range_str)
    if not match:
        raise ValueError(f"Invalid time range format: {range_str}. Use presets like '1h', '7d', '3mo' or custom like '45m', '6h'.")
    
    value = int(match.group(1))
    unit = match.group(2)
    
    # Convert to seconds
    unit_seconds = {
        "m": MINUTE,
        "h": HOUR,
        "d": DAY,
        "w": WEEK,
        "mo": MONTH,
        "y": YEAR,
        "yr": YEAR,
    }
    
    total_seconds = value * unit_seconds[unit]
    
    # Calculate appropriate bucket size (~200-400 buckets target)
    target_buckets = 300
    bucket_seconds = max(10, total_seconds // target_buckets)
    
    # Round bucket size to nice values
    if bucket_seconds < 60:
        bucket_seconds = (bucket_seconds // 10) * 10 or 10
    elif bucket_seconds < 3600:
        bucket_seconds = (bucket_seconds // 60) * 60
    elif bucket_seconds < 86400:
        bucket_seconds = (bucket_seconds // 3600) * 3600
    else:
        bucket_seconds = (bucket_seconds // 86400) * 86400
    
    # Generate label
    unit_labels = {
        "m": ("Minute", "Minutes"),
        "h": ("Hour", "Hours"),
        "d": ("Day", "Days"),
        "w": ("Week", "Weeks"),
        "mo": ("Month", "Months"),
        "y": ("Year", "Years"),
        "yr": ("Year", "Years"),
    }
    singular, plural = unit_labels[unit]
    label = f"{value} {singular if value == 1 else plural}"
    
    return (total_seconds, bucket_seconds, label)


def get_available_presets() -> List[dict]:
    """
    Get list of available time range presets for UI.
    
    Returns:
        List of dicts with 'key', 'label', 'seconds', 'bucket_seconds'
    """
    result = []
    for key in TIME_RANGE_OPTIONS:
        total_secs, bucket_secs, label = TIME_RANGE_PRESETS[key]
        result.append({
            "key": key,
            "label": label,
            "total_seconds": total_secs,
            "bucket_seconds": bucket_secs,
            "bucket_count": total_secs // bucket_secs,
        })
    return result


def get_airtime_utilization(
    conn_or_db: DBConnection,
    minutes: int = 60,
    bucket_count: int = 360,
    radio_config: Optional[dict] = None,
) -> BucketedStats:
    """
    Get airtime utilization data for the last N minutes.
    
    Convenience wrapper around aggregate_packet_buckets.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        minutes: Time range in minutes
        bucket_count: Number of buckets to return
        radio_config: Radio config for airtime calculation
        
    Returns:
        BucketedStats for API response
    """
    end_ts = int(time.time())
    start_ts = end_ts - (minutes * 60)
    bucket_seconds = max(1, (minutes * 60) // bucket_count)
    
    return aggregate_packet_buckets(
        conn_or_db=conn_or_db,
        start_ts=start_ts,
        end_ts=end_ts,
        bucket_seconds=bucket_seconds,
        radio_config=radio_config,
    )


def get_airtime_utilization_preset(
    conn_or_db: DBConnection,
    preset: str = "1h",
    radio_config: Optional[dict] = None,
) -> BucketedStats:
    """
    Get airtime utilization using a time range preset.
    
    Args:
        conn_or_db: SQLite connection or AnalyticsDB instance
        preset: Time range preset key (e.g. "1h", "7d", "3mo")
        radio_config: Radio config for airtime calculation
        
    Returns:
        BucketedStats for API response
    """
    total_seconds, bucket_seconds, label = parse_time_range(preset)
    
    end_ts = int(time.time())
    start_ts = end_ts - total_seconds
    
    stats = aggregate_packet_buckets(
        conn_or_db=conn_or_db,
        start_ts=start_ts,
        end_ts=end_ts,
        bucket_seconds=bucket_seconds,
        radio_config=radio_config,
    )
    
    # Add preset info
    stats.preset = preset
    stats.preset_label = label
    
    return stats
