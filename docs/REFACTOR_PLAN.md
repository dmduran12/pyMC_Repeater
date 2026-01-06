# Analytics System: Problem Report & Refactoring Plan

## Executive Summary

The analytics implementation has several issues ranging from **critical import errors** to **API inconsistencies** and **missing type safety**. This document catalogs all identified problems and provides a prioritized refactoring path.

---

## Critical Issues (P0) - Blocking

### 1. Broken `__init__.py` Exports

**Location:** `repeater/analytics/__init__.py:114-117`

**Problem:** The module exports `PrefixCandidate` and `ResolvedPrefix` which don't exist in `disambiguation.py`. The actual classes are:
- `DisambiguationCandidate`
- `DisambiguationResult`

**Impact:** **All analytics imports fail.** The entire analytics module is currently broken.

```python
# BROKEN - these classes don't exist
from .disambiguation import (
    PrefixCandidate,      # ✗ Should be DisambiguationCandidate
    ResolvedPrefix,       # ✗ Should be DisambiguationResult
)
```

**Fix:**
```python
from .disambiguation import (
    DisambiguationCandidate,
    DisambiguationResult,
    DisambiguationStats,
    PrefixLookup,
)
```

---

### 2. `max_flow()` Return Type Mismatch

**Location:** 
- `graph.py:844` - Returns `float`
- `analytics_api.py:1547` - Calls `.to_dict()` on the result

**Problem:** The `max_flow()` method returns a `float`, but the API endpoint tries to call `.to_dict()` on it.

```python
# graph.py line 844
def max_flow(self, source: str, target: str) -> float:
    ...
    return float(flow_value)

# analytics_api.py line 1547 - WILL CRASH
result = graph.max_flow(source, target)
return self._success(result.to_dict())  # ✗ float has no .to_dict()
```

**Impact:** `GET /api/analytics/graph_max_flow` will crash with `AttributeError`.

**Fix:** Either:
1. Change `max_flow()` to return a dataclass with `to_dict()`, or
2. Change the API endpoint to handle the float:
```python
result = graph.max_flow(source, target)
return self._success({
    "source": source,
    "target": target,
    "maxFlowValue": result,
    "pathExists": result > 0,
})
```

---

## High Priority Issues (P1) - Functional Bugs

### 3. Frontend Type Mismatch for MaxFlow

**Location:** `pymc_console/frontend/src/lib/api-analytics.ts:1202-1215`

**Problem:** TypeScript expects `flowEdges` array but backend doesn't provide it.

```typescript
// Frontend expects this
export interface MaxFlowResponse {
  maxFlowValue: number;
  flowEdges: MaxFlowEdge[];  // Backend doesn't provide this
  pathExists: boolean;
}
```

**Fix:** Either update backend to provide flow edges, or remove from TypeScript interface.

---

### 4. Missing `ensure_connection` Implementation Check

**Location:** Multiple files use `ensure_connection` decorator

**Problem:** The `ensure_connection` decorator/function is used inconsistently:
- Some functions use `@ensure_connection` decorator
- Some use `with ensure_connection(conn_or_db) as conn:`
- The decorator implementation may not handle raw connections properly

**Example inconsistency:**
```python
# geographic_coverage.py - uses decorator
@ensure_connection
def get_geographic_coverage(conn: DBConnection, ...):

# graph.py - uses context manager
with ensure_connection(conn_or_db) as conn:
```

---

### 5. SQL Injection Risk in Hash Normalization

**Location:** Multiple files

**Problem:** Hash normalization does string concatenation without validation:

```python
# route_simulation.py line 179
if not node.startswith("0X"):
    node = f"0x{node}"  # Direct string concat
```

User input is being used directly. While SQLite parameterization protects SQL, the hash could contain unexpected characters.

**Fix:** Add validation:
```python
def _normalize_hash(node: str) -> str:
    node = node.strip().upper()
    # Validate hex characters only
    clean = ''.join(c for c in node if c in '0123456789ABCDEFX')
    if not clean.startswith("0X"):
        clean = f"0x{clean}"
    return clean
```

---

## Medium Priority Issues (P2) - Code Quality

### 6. Duplicate Hash Normalization Functions

**Location:** Multiple files define similar functions:
- `route_simulation.py:176` - `_normalize_hash()`
- `node_profile.py:199` - `_normalize_node_hash()`
- `edge_builder.py` - `get_hash_prefix()`

**Problem:** Code duplication leads to inconsistent behavior.

**Fix:** Centralize in `edge_builder.py` or create `utils.py`:
```python
# repeater/analytics/utils.py
def normalize_hash(node: str) -> str:
    """Normalize node hash to 0xXXXXXXXX format."""
    ...

def get_prefix(node_hash: str) -> str:
    """Extract 2-char prefix from hash."""
    ...

def validate_hash(node: str) -> bool:
    """Validate hash format."""
    ...
```

---

### 7. Inconsistent Error Handling

**Location:** API endpoints in `analytics_api.py`

**Problem:** Some endpoints return different error formats:
```python
# Some use this
return self._error(f"Invalid parameter: {e}")

# Some use this  
return self._error(e)  # Just the exception
```

**Fix:** Standardize error responses:
```python
def _error(self, error: Union[str, Exception], code: str = None) -> dict:
    msg = str(error) if isinstance(error, Exception) else error
    return {
        "success": False,
        "error": msg,
        "errorCode": code,
    }
```

---

### 8. Missing Database Connection Cleanup

**Location:** `analytics_api.py:83-85`

**Problem:** `_get_connection()` uses raw `sqlite3.connect()` without the AnalyticsDB pragmas:

```python
def _get_connection(self):
    """Get SQLite connection."""
    return sqlite3.connect(self.sqlite_path)  # No WAL, no pragmas!
```

**Impact:** API queries don't benefit from WAL mode, connection pooling, or optimized pragmas.

**Fix:** Use AnalyticsDB instance:
```python
def __init__(self, sqlite_path, ...):
    self.db = AnalyticsDB(sqlite_path)

def _get_connection(self):
    return self.db.connection()
```

---

### 9. Hardcoded Magic Numbers

**Location:** Throughout analytics modules

**Examples:**
```python
# geographic_coverage.py
GRID_RESOLUTION_KM = 5.0
MAX_COVERAGE_RADIUS_KM = 50.0

# route_simulation.py  
BASE_LATENCY_PER_HOP_MS = 250
PROCESSING_DELAY_MS = 50

# network_health.py
CRITICAL_NODE_TRAFFIC_THRESHOLD = 0.3
WEAK_LINK_CONFIDENCE_THRESHOLD = 0.4
```

**Problem:** These values should be configurable, not hardcoded.

**Fix:** Create a config module:
```python
# repeater/analytics/config.py
from dataclasses import dataclass

@dataclass
class AnalyticsConfig:
    # Geographic
    grid_resolution_km: float = 5.0
    max_coverage_radius_km: float = 50.0
    
    # Latency estimation
    base_latency_per_hop_ms: int = 250
    processing_delay_ms: int = 50
    
    # Health thresholds
    critical_traffic_threshold: float = 0.3
    weak_link_threshold: float = 0.4
```

---

### 10. Missing Input Validation

**Location:** API endpoints

**Problem:** Parameters are cast with `int()` or `float()` without validation:

```python
def geo_coverage(self, hours=168, grid_resolution=5, num_suggestions=5):
    hours = int(hours)  # Can throw ValueError
    grid_resolution = float(grid_resolution)  # Can be negative!
```

**Fix:** Add validation:
```python
def _validate_positive_int(value, name: str, default: int, max_val: int = None) -> int:
    try:
        result = int(value)
        if result <= 0:
            raise ValueError(f"{name} must be positive")
        if max_val and result > max_val:
            raise ValueError(f"{name} cannot exceed {max_val}")
        return result
    except (TypeError, ValueError):
        return default
```

---

## Low Priority Issues (P3) - Improvements

### 11. Missing Test Coverage

**Problem:** No unit tests for new analytics modules.

**Needed:**
- `test_graph.py` - Graph algorithm tests
- `test_node_profile.py` - Profile generation tests
- `test_network_health.py` - Health scoring tests
- `test_geographic_coverage.py` - Spatial analysis tests

---

### 12. Missing Type Hints in Some Functions

**Location:** Various

**Example:**
```python
# Some functions missing return types
def _get_node_info(conn: DBConnection):  # Missing -> Dict[str, Tuple[str, Optional[str]]]
```

---

### 13. Inefficient Graph Rebuilding

**Problem:** Many endpoints rebuild the graph from scratch for each request.

**Current:**
```python
with self._get_connection() as conn:
    graph = build_graph_from_db(conn, ...)  # Full rebuild
    bridges = graph.find_bridges()
```

**Fix:** Add graph caching with TTL:
```python
class GraphCache:
    def __init__(self, ttl_seconds: int = 60):
        self._cache = {}
        self._ttl = ttl_seconds
    
    def get_or_build(self, conn, min_certainty: int) -> MeshGraph:
        key = min_certainty
        if key in self._cache:
            graph, built_at = self._cache[key]
            if time.time() - built_at < self._ttl:
                return graph
        
        graph = build_graph_from_db(conn, min_certainty)
        self._cache[key] = (graph, time.time())
        return graph
```

---

### 14. Documentation Inconsistencies

**Problem:** Some docstrings reference wrong line numbers or outdated information.

**Example:** `docs/analytics.md` references line numbers that may drift.

**Fix:** Remove specific line number references or use anchor comments.

---

## Refactoring Roadmap

### Phase 1: Critical Fixes (Immediate)

1. **Fix `__init__.py` exports** - 5 min
   - Update imports to use actual class names

2. **Fix `max_flow()` return type** - 15 min
   - Create `MaxFlowResult` dataclass
   - Update method to return it
   - Update API endpoint

### Phase 2: Stability (Week 1)

3. **Centralize hash utilities** - 1 hr
   - Create `repeater/analytics/utils.py`
   - Move all hash functions there
   - Update all imports

4. **Fix AnalyticsDB usage in API** - 30 min
   - Use AnalyticsDB instead of raw sqlite3.connect

5. **Standardize error handling** - 1 hr
   - Update `_error()` method
   - Add error codes

6. **Add input validation** - 2 hrs
   - Create validation helpers
   - Apply to all endpoints

### Phase 3: Quality (Week 2)

7. **Create config module** - 2 hrs
   - Extract all magic numbers
   - Make configurable

8. **Add graph caching** - 3 hrs
   - Implement GraphCache
   - Add cache invalidation

9. **Add comprehensive tests** - 8 hrs
   - Unit tests for each module
   - Integration tests for API

### Phase 4: Polish (Week 3)

10. **Update TypeScript types** - 2 hrs
    - Ensure frontend/backend parity
    - Add missing types

11. **Documentation update** - 2 hrs
    - Fix line number references
    - Add API examples

12. **Performance profiling** - 4 hrs
    - Identify slow endpoints
    - Add query optimization

---

## Quick Fix Checklist

- [x] Fix `__init__.py` broken imports ✅ COMPLETED
- [x] Fix `max_flow()` return type mismatch ✅ COMPLETED (created MaxFlowResult dataclass)
- [x] Fix `_get_connection()` to use AnalyticsDB ✅ COMPLETED
- [x] Add hash validation to normalization functions ✅ COMPLETED (utils.py)
- [ ] Update frontend `MaxFlowResponse` type (backend matches, frontend already correct)
- [x] Add basic input validation to API endpoints ✅ COMPLETED (validation.py)

---

## Refactoring Progress (Updated)

### Completed Items ✅

1. **P0-1: Fixed `__init__.py` exports** - Changed to actual class names
2. **P0-2: Created `MaxFlowResult` dataclass** - Full return type with `to_dict()` method
3. **P1-1: Created centralized `utils.py`** - Hash normalization, validation, edge key utilities
4. **P1-2: Fixed `ensure_connection` consistency** - Added `with_connection` decorator
5. **P2-1: Refactored AnalyticsAPI** - Now uses AnalyticsDB instead of raw sqlite3.connect
6. **P2-2: Created `errors.py`** - Standardized error codes and response helpers
7. **P2-3: Created `config.py`** - Centralized configurable constants with env override
8. **P2-4: Created `validation.py`** - Comprehensive input validators

### Remaining Items

- P3-1: Graph caching with TTL
- P3-2: TypeScript frontend type verification
- P3-3: Type hints audit
- P3-4: Test coverage

---

## Files Requiring Changes

| File | Priority | Issues |
|------|----------|--------|
| `__init__.py` | P0 | Broken exports |
| `graph.py` | P0, P2 | max_flow return type, missing dataclass |
| `analytics_api.py` | P0, P1, P2 | max_flow call, DB connection, error handling |
| `api-analytics.ts` | P1 | MaxFlow type mismatch |
| `route_simulation.py` | P2 | Hash normalization |
| `node_profile.py` | P2 | Hash normalization |
| `geographic_coverage.py` | P3 | Magic numbers |
| `network_health.py` | P3 | Magic numbers |

---

## Estimated Effort

| Phase | Effort | Risk |
|-------|--------|------|
| Phase 1: Critical | 30 min | Low |
| Phase 2: Stability | 5 hrs | Medium |
| Phase 3: Quality | 13 hrs | Low |
| Phase 4: Polish | 8 hrs | Low |
| **Total** | **~27 hrs** | |
