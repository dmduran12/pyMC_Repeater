# pyMC_Repeater Analytics System

## Overview

The pyMC_Repeater analytics system provides comprehensive mesh network analysis using **SQLite for persistence** and **NetworkX for in-memory graph algorithms**. This architecture enables powerful graph computations without requiring an external graph database.

### Key Design Principles

- **Enterprise-grade error handling**: Standardized error codes, consistent response formats
- **Performance-focused caching**: TTL-based graph cache with race condition prevention
- **Type-safe validation**: Centralized input validation with defense-in-depth
- **Hot-reloadable configuration**: Change settings at runtime without restart
- **Thread-safe operations**: All caching and config operations are thread-safe

```
                    ┌─────────────────┐
                    │   Packet Data   │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   SQLite DB     │
                    │  (WAL mode)     │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │                             │
     ┌────────▼────────┐           ┌────────▼────────┐
     │   Graph Cache   │           │    NetworkX     │
     │  (TTL-based)    │◄─────────►│  (on-demand)    │
     └────────┬────────┘           └─────────────────┘
              │
   ┌──────────┴──────────────────────────────┐
   │                                         │
   │  ┌─────────────────────────────────┐   │
   │  │     Validation + Error Layer    │   │
   │  └─────────────┬───────────────────┘   │
   │                │                        │
   └────────────────┼────────────────────────┘
                    │
          ┌─────────┼─────────┐
          │         │         │
   ┌──────▼───┐ ┌───▼───┐ ┌───▼──────┐
   │ REST API │ │Worker │ │ Frontend │
   │(CherryPy)│ │ (bg)  │ │  Client  │
   └──────────┘ └───────┘ └──────────┘
```

## Architecture

### Data Flow

1. **Packet Reception**: The repeater receives mesh packets and stores them in SQLite (WAL mode for concurrency)
2. **Edge Extraction**: `edge_builder.py` extracts topology edges from packet paths
3. **Validation**: Input parameters validated via `validation.py` with defense-in-depth sanitization
4. **Graph Construction**: `cache.py` checks TTL-based cache, builds via `graph.py` if miss
5. **Analysis**: Analytics modules compute metrics from the cached graph
6. **Error Handling**: All errors wrapped via `errors.py` with standardized codes
7. **API Exposure**: `analytics_api.py` exposes results via CherryPy REST endpoints
8. **Frontend Consumption**: TypeScript client in `api-analytics.ts` consumes the API

### Database Schema

The analytics system uses several SQLite tables:

| Table | Purpose |
|-------|---------|
| `packets` | Raw packet data with paths, RSSI, SNR |
| `adverts` | Node advertisements with names, locations |
| `edge_observations` | Extracted topology edges with confidence |
| `path_observations` | Observed packet paths |
| `topology_snapshot` | Cached topology computations |
| `metric_history` | Time-series metric storage |
| `network_events` | Network event log |
| `graph_metrics_cache` | Cached graph computations |

---

## Infrastructure Modules

### Database (`db.py`)

Centralized database connection manager with WAL mode optimization.

**Key Features:**
- WAL (Write-Ahead Logging) for concurrent reads/writes
- Thread-safe connection management
- Row factory returns `sqlite3.Row` for key access
- Auto-commit with rollback on exception

```python
from repeater.analytics.db import AnalyticsDB

db = AnalyticsDB("/path/to/repeater.db")

with db.connection() as conn:
    cursor = conn.execute("SELECT * FROM packets WHERE timestamp > ?")
    for row in cursor:
        print(row["src_hash"], row["timestamp"])  # Key access
```

---

### Configuration (`config.py`)

Centralized, hot-reloadable configuration with environment variable overrides.

**Config Groups:**

| Group | Purpose | Example Setting |
|-------|---------|----------------|
| `TOPOLOGY` | Edge validation thresholds | `MIN_EDGE_VALIDATIONS=15` |
| `GRAPH` | Graph construction defaults | `DEFAULT_MIN_CERTAIN_COUNT=5` |
| `HEALTH` | Health scoring thresholds | `CRITICAL_NODE_TRAFFIC_THRESHOLD=0.3` |
| `NODE` | Node classification | `HUB_CENTRALITY_THRESHOLD=0.3` |
| `CACHE` | Graph cache settings | `GRAPH_CACHE_TTL_SECONDS=300` |
| `API` | API defaults and limits | `DEFAULT_HOURS=168`, `MAX_LIMIT=1000` |

**Usage:**
```python
from repeater.analytics.config import Config, reload_config

# Access config
print(Config.API.DEFAULT_HOURS)  # 168
print(Config.CACHE.GRAPH_CACHE_TTL_SECONDS)  # 300

# Hot-reload after changing env vars
import os
os.environ['ANALYTICS_API_DEFAULT_HOURS'] = '24'
reload_config()
print(Config.API.DEFAULT_HOURS)  # 24

# Export for debugging
print(Config.to_dict())
```

**Environment Override Pattern:** `ANALYTICS_{GROUP}_{NAME}`
```bash
export ANALYTICS_CACHE_GRAPH_TTL=600
export ANALYTICS_API_MAX_LIMIT=500
```

---

### Validation (`validation.py`)

Type-safe input validation with defense-in-depth sanitization.

**Validators:**

| Function | Purpose | Example |
|----------|---------|--------|
| `validate_hours(value)` | Time range validation | `validate_hours("24")` → `24` |
| `validate_limit(value)` | Pagination limits | `validate_limit("100")` → `100` |
| `validate_min_certainty(value)` | Edge certainty | `validate_min_certainty("5")` → `5` |
| `validate_hash_param(value, name)` | Hash format + sanitize | `validate_hash_param("0xAB!", "node")` → raises |
| `validate_bool(value, name)` | Boolean conversion | `validate_bool("true", "rebuild")` → `True` |
| `validate_string_choice(value, name, choices)` | Enum validation | See example |

**Usage:**
```python
from repeater.analytics.validation import (
    validate_hours, validate_hash_param, ValidationError
)

try:
    hours = validate_hours(request.param)  # Defaults to 168 if None
    node = validate_hash_param(request.node, "node")  # Sanitizes + normalizes
except ValidationError as e:
    return e.to_response()  # Returns standardized error format
```

---

### Error Handling (`errors.py`)

Standardized error codes and response formatting.

**Error Codes:**

| Code | HTTP Status | Usage |
|------|-------------|-------|
| `INVALID_PARAMETER` | 400 | Bad input |
| `MISSING_PARAMETER` | 400 | Required param missing |
| `NOT_FOUND` | 404 | Resource doesn't exist |
| `DEPENDENCY_MISSING` | 424 | NetworkX not installed |
| `DATABASE_ERROR` | 500 | DB operation failed |
| `INTERNAL_ERROR` | 500 | Unexpected error |

**Response Format:**
```json
{
  "success": false,
  "error": {
    "code": "INVALID_PARAMETER",
    "message": "'hours' must be at least 1, got -1",
    "httpStatus": 400,
    "details": {
      "parameter": "hours",
      "value": "-1"
    }
  }
}
```

**Usage:**
```python
from repeater.analytics.errors import (
    api_success, api_error, ErrorCode, 
    invalid_param, missing_param, not_found
)

# Success response
return api_success({"nodes": [...], "edges": [...]})

# Error responses
return api_error(ErrorCode.INVALID_PARAMETER, "Custom message")
return invalid_param("hours", "must be positive")
return missing_param("source")
return not_found("Node", "0xABCD1234")
```

---

### Graph Cache (`cache.py`)

TTL-based caching with race condition prevention.

**Key Features:**
- Cache key: `(min_certainty, local_hash)` tuple
- TTL-based expiration (default 5 minutes)
- LRU eviction when at capacity
- Building sentinel prevents duplicate builds
- Thread-safe with `RLock`

**Cache Flow:**
```
Request → Check Cache
           │
           ├── HIT (not expired) → Return cached graph
           │
           ├── BUILDING (another thread) → Wait for result
           │
           └── MISS → Place sentinel, build graph, notify waiters
```

**Usage:**
```python
from repeater.analytics.cache import (
    get_cached_graph, invalidate_graph_cache, get_cache_stats
)

# Get graph (cached or fresh)
with db.connection() as conn:
    graph = get_cached_graph(conn, min_certainty=5, local_hash="0xABCD")
    bridges = graph.find_bridges()

# Invalidate on topology changes
invalidate_graph_cache()  # All entries
invalidate_graph_cache(min_certainty=5)  # Specific certainty

# Monitor cache performance
stats = get_cache_stats()
print(f"Hit rate: {stats['stats']['hitRate']:.1%}")
print(f"Avg build time: {stats['stats']['avgBuildTimeMs']:.0f}ms")
```

**API Endpoints:**
- `GET /api/analytics/cache_info` - Cache stats and entries
- `POST /api/analytics/cache_invalidate` - Manual invalidation

---

### Hash Utilities (`utils.py`)

Centralized hash normalization and validation.

**Functions:**

| Function | Purpose | Example |
|----------|---------|--------|
| `normalize_hash(value)` | Normalize to `0xUPPERCASE` | `"abcd"` → `"0xABCD"` |
| `get_prefix(value)` | Extract 2-char prefix | `"0xABCD1234"` → `"AB"` |
| `validate_hash(value)` | Check format validity | `"0xABCD"` → `True` |
| `make_edge_key(a, b)` | Canonical edge key | `"0xCD", "0xAB"` → `"0xAB:0xCD"` |
| `hashes_match(a, b)` | Compare with prefix support | See below |
| `sanitize_for_sql(value)` | Remove non-hex chars | Defense-in-depth |

**`hashes_match()` Prefix Behavior:**
```python
# Exact match
hashes_match("0xABCD1234", "0xABCD1234")  # True

# Prefix matching (either side can be prefix)
hashes_match("0xABCD1234", "AB")  # True - prefix matches
hashes_match("AB", "0xABCD1234")  # True - prefix matches
hashes_match("AB", "AB")          # True - same prefix

# Non-matching
hashes_match("AB", "CD")          # False
```

> **Note:** When either input is a 2-char prefix, comparison is prefix-only.
> Use `normalize_hash()` and direct comparison for strict full-hash matching.

---

## Core Modules

### 1. Graph Analysis (`graph.py`)

The `MeshGraph` class wraps NetworkX with mesh-specific functionality.

**Key Methods:**

| Method | Description |
|--------|-------------|
| `shortest_path(src, dst)` | Find shortest path between nodes |
| `k_shortest_paths(src, dst, k)` | Find K best paths using Yen's algorithm |
| `betweenness_centrality()` | Calculate node importance for routing |
| `find_bridges()` | Detect critical edges (SPOFs) |
| `find_articulation_points()` | Detect critical nodes |
| `detect_communities()` | Find node clusters via modularity |
| `resilience_score()` | Calculate network resilience (0-1) |
| `max_flow(src, dst)` | Calculate theoretical max throughput |

**Example (using cache):**
```python
from repeater.analytics.cache import get_cached_graph

with db.connection() as conn:
    graph = get_cached_graph(conn, min_certainty=5)
    
    # Find path
    path = graph.shortest_path("0xAB123456", "0xCD789012")
    
    # Get critical infrastructure
    bridges = graph.find_bridges()
    articulation_points = graph.find_articulation_points()
```

**MaxFlowResult dataclass:**
```python
@dataclass
class MaxFlowResult:
    source: str
    target: str
    max_flow_value: float
    flow_edges: List[MaxFlowEdge]  # Individual edge flows
    path_exists: bool
```

---

### 2. Node Profile (`node_profile.py`)

Aggregates all available data about a single node into a comprehensive profile.

**Profile Components:**

| Component | Fields | Reference |
|-----------|--------|-----------|
| Identity | hash, prefix, name, is_repeater | Lines 126-131 |
| Network Role | role, is_hub, is_gateway, is_articulation_point | Lines 134-139 |
| Connectivity | degree, centrality, eccentricity, neighbors | Lines 142-145 |
| Traffic | paths_through, avg_hop_position, packet_count | Lines 148-150 |
| Activity | sparkline, last_seen, active_hours | Lines 153-156 |
| Signal Quality | avg_rssi, avg_snr, is_zero_hop | Lines 104-120 |

**Role Classification:**
- **Hub**: Centrality > 0.3 OR is in topology hub_nodes
- **Relay**: Centrality > 0.1 AND degree >= 3
- **Edge**: Low centrality, few connections
- **Backbone**: Is articulation point

**API Endpoint:** `GET /api/analytics/node_profile?node=0xAB123456`

---

### 3. Network Health (`network_health.py`)

Provides overall network health assessment with issue detection and recommendations.

**Health Score Components (25% each):**

| Component | Description | Reference |
|-----------|-------------|-----------|
| Connectivity | Are all nodes reachable? Component count | Lines 207-262 |
| Redundancy | Edge ratio vs MST, multiple paths | Lines 265-323 |
| Stability | Path consistency over time | Lines 326-382 |
| Coverage | Geographic distribution, signal quality | Lines 385-435 |

**Issue Detection:**

| Issue Type | Severity | Detection |
|------------|----------|-----------|
| Single Point of Failure | Critical | Articulation points with high traffic |
| Bridge Edge | High | Edges whose removal disconnects graph |
| Weak Link | Medium | Confidence < 40% |
| Asymmetric Link | Medium | Symmetry ratio < 30% |
| Overloaded Node | High | Handles >30% of traffic |
| Isolated Node | Low | Nodes with degree 0 |
| Fragmented Network | Critical | Multiple components |

**API Endpoints:**
- `GET /api/analytics/network_health` - Full report
- `GET /api/analytics/quick_health` - Score only (faster)

---

### 4. Route Simulation (`route_simulation.py`)

Predicts routing behavior between nodes before sending messages.

**Simulation Components:**

| Component | Description | Reference |
|-----------|-------------|-----------|
| Path Prediction | Uses shortest path + historical data | Lines 461-474 |
| Latency Estimation | Based on hop count + bottlenecks | Lines 184-216 |
| Success Probability | Product of edge confidences | Lines 273-302 |
| Bottleneck Detection | Articulation points, weak links | Lines 219-270 |
| Alternative Paths | K-shortest + historical paths | Lines 524-546 |

**Latency Model:**
```
Base = hop_count × 250ms
Processing = hop_count × 50ms  
TX Delay = hop_count × 100ms
Total = (Base + Processing + TX Delay) × bottleneck_factor × flood_factor
```

**API Endpoint:** `GET /api/analytics/simulate_route?source=0xAB&target=0xCD`

---

### 5. Metric History (`metric_history.py`)

Tracks metrics over time with trend analysis using linear regression.

**Tracked Metrics:**

| Metric | Scope | Description |
|--------|-------|-------------|
| `node_count` | Network | Total nodes |
| `edge_count` | Network | Total validated edges |
| `health_score` | Network | Overall health (0-1) |
| `resilience` | Network | Resilience score |
| `centrality` | Per-node | Betweenness centrality |
| `degree` | Per-node | Connection count |
| `traffic` | Per-node | Packet volume |

**Trend Detection:**

| Direction | Criteria |
|-----------|----------|
| Increasing | slope > 0.01 AND r² > 0.3 |
| Decreasing | slope < -0.01 AND r² > 0.3 |
| Stable | abs(slope) < 0.01 |
| Volatile | Coefficient of variation > 0.3 |

**API Endpoint:** `GET /api/analytics/trends?metric=health_score&days=7`

---

### 6. Geographic Coverage (`geographic_coverage.py`)

Analyzes spatial distribution of the mesh network.

**Analysis Components:**

| Component | Description | Reference |
|-----------|-------------|-----------|
| Convex Hull | Coverage boundary polygon | Lines 235-269 |
| Bounding Box | Min/max lat/lon | Lines 376-389 |
| Coverage Gaps | Grid analysis for weak areas | Lines 445-506 |
| Density Regions | Node concentration analysis | Lines 509-571 |
| Placement Suggestions | Optimal new node locations | Lines 574-621 |

**Coverage Score Formula:**
```
score = node_score - gap_penalty + area_bonus
where:
  node_score = min(1.0, node_count / 20)
  gap_penalty = avg_severity × 0.5
  area_bonus = min(0.1, area_km² / 10000)
```

**API Endpoints:**
- `GET /api/analytics/geo_coverage` - Full analysis
- `GET /api/analytics/geo_coverage_summary` - Quick summary

---

### 7. Packet Flow (`packet_flow.py`)

Aggregates traffic data for Sankey diagram visualization.

**Data Structures:**

| Structure | Purpose | Reference |
|-----------|---------|-----------|
| `SankeyData` | Nodes + links for visualization | Lines 101-134 |
| `FlowMatrix` | Source-destination matrix | Lines 167-179 |
| `TopTalker` | High-traffic nodes | Lines 137-164 |

**Sankey Format:**
```json
{
  "nodes": [{"hash": "0xAB...", "totalSent": 100, ...}],
  "links": [{"source": 0, "target": 1, "value": 50, ...}],
  "totalPackets": 1000
}
```

**API Endpoints:**
- `GET /api/analytics/packet_flow` - Sankey data
- `GET /api/analytics/flow_matrix` - Matrix format
- `GET /api/analytics/top_talkers` - Traffic ranking

---

## API Endpoint Reference

### High Priority Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/analytics/node_profile` | GET | Comprehensive node profile |
| `/api/analytics/network_health` | GET | Full health report |
| `/api/analytics/quick_health` | GET | Health score only |
| `/api/analytics/graph_bridges` | GET | Bridge edges (SPOFs) |
| `/api/analytics/graph_articulation_points` | GET | Critical nodes |

### Medium Priority Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/analytics/graph_k_paths` | GET | K shortest paths |
| `/api/analytics/graph_communities` | GET | Community detection |
| `/api/analytics/graph_resilience` | GET | Resilience score |
| `/api/analytics/simulate_route` | GET | Route prediction |
| `/api/analytics/trends` | GET | Metric trend analysis |

### Lower Priority Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/analytics/geo_coverage` | GET | Geographic coverage |
| `/api/analytics/packet_flow` | GET | Sankey flow data |
| `/api/analytics/flow_matrix` | GET | Flow matrix |
| `/api/analytics/top_talkers` | GET | Traffic ranking |
| `/api/analytics/graph_max_flow` | GET | Max flow calculation |

---

## Frontend TypeScript Client

The frontend client is in `pymc_console/frontend/src/lib/api-analytics.ts`.

### Type Definitions

```typescript
// Node Profile
interface NodeProfile {
  hash: string;
  prefix: string;
  role: 'repeater' | 'hub' | 'gateway' | 'edge' | 'unknown';
  centrality: number;
  neighbors: NodeNeighbor[];
  signalQuality: SignalQuality;
  // ... more fields
}

// Network Health
interface NetworkHealthReport {
  healthScore: number;
  metrics: HealthMetrics;
  issues: NetworkIssue[];
  recommendations: Recommendation[];
}

// Route Simulation
interface RouteSimulation {
  predictedPath: string[];
  estimatedLatencyMs: number;
  successProbability: number;
  bottlenecks: Bottleneck[];
  alternativePaths: AlternativePath[];
}
```

### API Functions

```typescript
// Node analysis
getNodeProfile(node: string, hours?: number): Promise<NodeProfile>

// Network health
getNetworkHealth(hours?: number): Promise<NetworkHealthReport>
getQuickHealth(): Promise<{healthScore: number}>

// Graph analysis
getGraphBridges(): Promise<BridgesResponse>
getGraphArticulationPoints(): Promise<ArticulationPointsResponse>
getGraphCommunities(): Promise<CommunitiesResponse>
getGraphResilience(): Promise<ResilienceResponse>
getKShortestPaths(source, target, k): Promise<KPathsResponse>

// Route simulation
simulateRoute(source, target, options?): Promise<RouteSimulation>

// Trends
getMetricTrend(metric, options?): Promise<TrendAnalysis>
getNetworkTrends(days?): Promise<NetworkTrendsResponse>

// Geographic
getGeographicCoverage(hours?): Promise<GeographicCoverage>

// Flow
getPacketFlows(hours?): Promise<SankeyData>
getTopTalkers(hours?, limit?): Promise<TopTalkersResponse>
getMaxFlow(source, target): Promise<MaxFlowResponse>
```

---

## Performance Considerations

### Caching Architecture

#### Graph Cache (in-memory)
The `cache.py` module provides TTL-based caching for `MeshGraph` instances:

| Setting | Default | Env Override |
|---------|---------|-------------|
| TTL | 300 seconds (5 min) | `ANALYTICS_CACHE_GRAPH_TTL` |
| Max entries | 10 | Constructor param |
| Cache key | `(min_certainty, local_hash)` | - |

**Race Condition Prevention:**
If multiple threads request the same uncached graph:
1. First thread places a "building" sentinel
2. Other threads wait on the sentinel's event
3. Builder completes and notifies all waiters
4. Result is shared (no duplicate builds)

#### Topology Snapshot (database)
Rebuild every 5 minutes by worker, stored in `topology_snapshot` table.

#### Automatic Invalidation
- Cache entries auto-expire after TTL
- Topology rebuild triggers `invalidate_graph_cache()`
- Manual invalidation via `/api/analytics/cache_invalidate`

### Computational Complexity

| Algorithm | Complexity | Notes |
|-----------|------------|-------|
| Shortest Path | O(V + E log V) | Dijkstra's |
| K-Shortest Paths | O(K × V × (E + V log V)) | Yen's algorithm |
| Betweenness Centrality | O(V × E) | Most expensive |
| Bridges | O(V + E) | Tarjan's algorithm |
| Community Detection | O(E × log V) | Greedy modularity |

### Recommended Usage

- Use `quick_health` for dashboard polling (uses cached graph)
- Use full `network_health` sparingly (multiple graph operations)
- Cache topology data on frontend for 5+ minutes
- Use `min_certainty` parameter to filter noise
- Monitor cache hit rate via `/api/analytics/cache_info`

### Monitoring Cache Performance

```bash
curl http://localhost:8080/api/analytics/cache_info
```

```json
{
  "success": true,
  "data": {
    "ttlSeconds": 300,
    "maxEntries": 10,
    "currentSize": 2,
    "buildingCount": 0,
    "entries": [
      {"minCertainty": 5, "localHash": "0xABCD", "ageSeconds": 45.2, "hits": 12}
    ],
    "stats": {
      "hits": 156,
      "misses": 14,
      "hitRate": 0.918,
      "avgBuildTimeMs": 234.5,
      "evictions": 2
    }
  }
}
```

---

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| NetworkX | ≥3.0 | Graph algorithms |
| SQLite3 | Built-in | Data persistence |
| CherryPy | ≥18.0 | REST API server |

Install NetworkX: `pip install networkx>=3.0`

---

## File Structure

```
repeater/analytics/
├── __init__.py              # Public exports (60+ symbols)
│
├── # Infrastructure
├── db.py                    # Database connection manager (WAL mode)
├── config.py                # Hot-reloadable configuration
├── errors.py                # Standardized error codes
├── validation.py            # Input validation + sanitization
├── cache.py                 # TTL-based graph caching
├── utils.py                 # Hash normalization utilities
│
├── # Core Analysis
├── graph.py                 # NetworkX graph wrapper (MeshGraph)
├── topology.py              # Topology snapshot computation
├── edge_builder.py          # Edge extraction from packets
├── path_registry.py         # Path tracking and analysis
│
├── # Node Analysis
├── node_profile.py          # Single-node comprehensive profile
├── node_activity.py         # Sparkline generation
├── disambiguation.py        # Prefix collision resolution
├── mobile_detection.py      # Mobile node classification
│
├── # Network Analysis
├── network_health.py        # Network health dashboard
├── route_simulation.py      # Route prediction/simulation
├── metric_history.py        # Time-series trend analysis
│
├── # Geographic
├── geographic_coverage.py   # Spatial coverage analysis
├── geo_utils.py             # Distance calculations
│
├── # Traffic Analysis
├── packet_flow.py           # Sankey flow data
├── last_hop.py              # Direct RF neighbor detection
├── neighbor_affinity.py     # Relationship scoring
├── tx_delay.py              # TX delay recommendations
└── bucketing.py             # Time-series bucketing

repeater/web/
└── analytics_api.py         # CherryPy REST endpoints (50+ endpoints)

pymc_console/frontend/src/lib/
└── api-analytics.ts         # TypeScript API client + types
```

---

## Version History

### feat/sql branch

**Initial Implementation:**
- SQLite+NetworkX architecture
- Node profile, network health, route simulation
- Geographic coverage, packet flow Sankey
- K-shortest paths, community detection, max flow
- Time-series trend analysis with linear regression

**Enterprise-Grade Refactoring:**
- Added `errors.py`: Standardized error codes (`ErrorCode` enum) and response helpers
- Added `validation.py`: Type-safe input validation with defense-in-depth sanitization
- Added `config.py`: Hot-reloadable configuration with `reload_config()`
- Added `cache.py`: TTL-based graph caching with race condition prevention
- Enhanced `utils.py`: Centralized hash utilities with clear documentation
- Enhanced `db.py`: WAL mode, `RowType` alias, improved docstrings
- Refactored `analytics_api.py`: 
  - Migrated 50+ endpoints to `api_success()`/`api_error()`
  - Integrated validation module for all parameters
  - Integrated graph cache for all graph endpoints
  - Added `/api/analytics/cache_info` and `/api/analytics/cache_invalidate`
- Fixed `__init__.py`: Single export for `make_edge_key`, added `reload_config`
- Updated TypeScript types: Added `ErrorCode`, `CacheStats`, `ApiErrorResponse`
