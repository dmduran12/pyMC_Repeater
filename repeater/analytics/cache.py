"""
Graph Caching for Analytics
===========================

Provides TTL-based caching for expensive graph computations.
Avoids rebuilding the NetworkX graph on every API request.

Usage
-----
    from repeater.analytics.cache import GraphCache, get_cached_graph
    
    # Using the module-level cache
    graph = get_cached_graph(conn, min_certainty=5)
    
    # Or create a custom cache
    cache = GraphCache(ttl_seconds=300)
    graph = cache.get_or_build(conn, min_certainty=5)

Cache Keys
----------
    Cache entries are keyed by (min_certainty, local_hash) tuple.
    This ensures different local_hash values get separate cached graphs.

Cache Invalidation
-----------------
    The cache automatically invalidates entries after TTL expires.
    Manual invalidation is also available:
    
        cache.invalidate()                            # Clear all
        cache.invalidate(min_certainty=5)             # Clear specific certainty
        cache.invalidate(min_certainty=5, local_hash="0xAB")  # Clear specific key

Thread Safety
-------------
    The cache uses threading locks for safe concurrent access.
    Multiple requests can safely share the cached graph.
    
    Race Condition Prevention:
    If multiple threads request the same uncached graph simultaneously,
    only one will build it. Others will wait and receive the built result.
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple, Any, Union

from .config import Config
from .db import DBConnection, ensure_connection

logger = logging.getLogger("Analytics.Cache")

# Import graph builder - handle case where NetworkX unavailable
try:
    from .graph import build_graph_from_db, MeshGraph, NETWORKX_AVAILABLE
except ImportError:
    NETWORKX_AVAILABLE = False
    MeshGraph = None
    build_graph_from_db = None


# Sentinel value indicating a graph is currently being built
# This prevents duplicate builds when multiple threads request the same cache key
class _BuildingSentinel:
    """Sentinel indicating a cache entry is being built."""
    def __init__(self):
        self.event = threading.Event()
        self.result: Optional[Any] = None
        self.error: Optional[Exception] = None


# Type alias for cache keys: (min_certainty, local_hash or empty string)
CacheKey = Tuple[int, str]


@dataclass
class CacheEntry:
    """A cached item with metadata."""
    value: Any
    created_at: float
    local_hash: str = ""  # Track which local_hash this was built with
    hits: int = 0
    
    def is_expired(self, ttl_seconds: float) -> bool:
        """Check if entry has expired."""
        return time.time() - self.created_at > ttl_seconds
    
    def age_seconds(self) -> float:
        """Get age of entry in seconds."""
        return time.time() - self.created_at


class GraphCache:
    """
    TTL-based cache for MeshGraph instances.
    
    Caches graphs by (min_certainty, local_hash) tuple to avoid expensive
    rebuilds on every request while correctly handling different local nodes.
    
    Attributes:
        ttl_seconds: Time-to-live for cache entries
        max_entries: Maximum number of cached graphs (LRU eviction)
        
    Thread Safety:
        Uses lock + sentinel pattern to prevent duplicate builds.
        If thread A is building a graph and thread B requests the same key,
        thread B will wait for A's result rather than building a duplicate.
        
    Example:
        >>> cache = GraphCache(ttl_seconds=300)
        >>> graph = cache.get_or_build(conn, min_certainty=5)
        >>> # Subsequent calls return cached graph
        >>> graph2 = cache.get_or_build(conn, min_certainty=5)
        >>> graph is graph2  # True (same instance)
    """
    
    def __init__(
        self,
        ttl_seconds: Optional[int] = None,
        max_entries: int = 10,
    ):
        """
        Initialize graph cache.
        
        Args:
            ttl_seconds: Cache TTL (default from Config.CACHE.GRAPH_CACHE_TTL_SECONDS)
            max_entries: Maximum cached graphs before LRU eviction
        """
        self._ttl = ttl_seconds or Config.CACHE.GRAPH_CACHE_TTL_SECONDS
        self._max_entries = max_entries
        self._cache: Dict[CacheKey, Union[CacheEntry, _BuildingSentinel]] = {}
        self._lock = threading.RLock()
        self._stats = CacheStats()
    
    def _make_cache_key(self, min_certainty: int, local_hash: Optional[str]) -> CacheKey:
        """Create cache key from parameters."""
        # Normalize local_hash to empty string if None for consistent keying
        return (min_certainty, local_hash or "")
    
    def get_or_build(
        self,
        conn_or_db: DBConnection,
        min_certainty: int = 5,
        local_hash: Optional[str] = None,
        force_rebuild: bool = False,
    ) -> Optional['MeshGraph']:
        """
        Get graph from cache or build new one.
        
        Thread Safety:
            If multiple threads request the same uncached key, only one
            will build the graph. Others will wait and receive the result.
        
        Args:
            conn_or_db: Database connection
            min_certainty: Minimum certainty for edges (part of cache key)
            local_hash: Local node hash for graph building (part of cache key)
            force_rebuild: If True, bypass cache and rebuild
            
        Returns:
            MeshGraph instance, or None if NetworkX unavailable
        """
        if not NETWORKX_AVAILABLE:
            logger.warning("NetworkX not available, cannot build graph")
            return None
        
        cache_key = self._make_cache_key(min_certainty, local_hash)
        
        # Fast path: check cache
        with self._lock:
            if not force_rebuild and cache_key in self._cache:
                entry = self._cache[cache_key]
                
                # If another thread is building, wait for it
                if isinstance(entry, _BuildingSentinel):
                    sentinel = entry
                else:
                    # It's a CacheEntry
                    if not entry.is_expired(self._ttl):
                        entry.hits += 1
                        self._stats.hits += 1
                        logger.debug(
                            f"Graph cache hit: key={cache_key}, "
                            f"age={entry.age_seconds():.1f}s, hits={entry.hits}"
                        )
                        return entry.value
                    else:
                        # Expired - will rebuild
                        del self._cache[cache_key]
                        self._stats.expirations += 1
                        sentinel = None
            else:
                sentinel = None
            
            # If another thread is building, wait outside lock
            if sentinel is None and cache_key not in self._cache:
                # We'll be the builder - place sentinel
                sentinel = _BuildingSentinel()
                self._cache[cache_key] = sentinel
                self._stats.misses += 1
        
        # If we found a sentinel (someone else building), wait for result
        if sentinel is not None and not isinstance(self._cache.get(cache_key), _BuildingSentinel):
            # Already completed while we were checking
            with self._lock:
                entry = self._cache.get(cache_key)
                if isinstance(entry, CacheEntry):
                    return entry.value
        elif sentinel is not None and sentinel.event.wait(timeout=60.0):
            # Wait for builder to finish (60s timeout)
            if sentinel.error:
                raise sentinel.error
            return sentinel.result
        elif sentinel is not None:
            # We placed the sentinel, we're the builder
            pass
        else:
            # We need to build
            with self._lock:
                if cache_key not in self._cache:
                    sentinel = _BuildingSentinel()
                    self._cache[cache_key] = sentinel
                    self._stats.misses += 1
                else:
                    # Someone else added it, try again
                    return self.get_or_build(conn_or_db, min_certainty, local_hash, force_rebuild)
        
        # Build graph (outside lock - this is the expensive part)
        logger.debug(f"Building graph: min_certainty={min_certainty}, local_hash={local_hash}")
        start_time = time.time()
        graph = None
        error = None
        
        try:
            with ensure_connection(conn_or_db) as conn:
                graph = build_graph_from_db(conn, min_certainty, local_hash)
            
            build_time = time.time() - start_time
            logger.debug(
                f"Graph built: {graph.node_count} nodes, {graph.edge_count} edges, "
                f"took {build_time*1000:.1f}ms"
            )
        except Exception as e:
            error = e
            logger.error(f"Graph build failed: {e}")
        
        # Store result and notify waiters
        with self._lock:
            current = self._cache.get(cache_key)
            if isinstance(current, _BuildingSentinel):
                current.result = graph
                current.error = error
                current.event.set()  # Wake up waiters
                
                if error:
                    # Remove sentinel on error
                    del self._cache[cache_key]
                else:
                    # Evict if at capacity
                    # Count only CacheEntry items, not sentinels
                    cache_count = sum(1 for v in self._cache.values() if isinstance(v, CacheEntry))
                    if cache_count >= self._max_entries:
                        self._evict_oldest()
                    
                    # Replace sentinel with real entry
                    build_time = time.time() - start_time
                    self._cache[cache_key] = CacheEntry(
                        value=graph,
                        created_at=time.time(),
                        local_hash=local_hash or "",
                    )
                    self._stats.builds += 1
                    self._stats.total_build_time += build_time
        
        if error:
            raise error
        return graph
    
    def invalidate(
        self,
        min_certainty: Optional[int] = None,
        local_hash: Optional[str] = None,
    ) -> int:
        """
        Invalidate cache entries.
        
        Args:
            min_certainty: Specific certainty to invalidate
            local_hash: Specific local_hash to invalidate
            
            If both provided: invalidate that specific key
            If only min_certainty: invalidate all entries with that certainty
            If neither: invalidate all entries
            
        Returns:
            Number of entries invalidated
        """
        with self._lock:
            if min_certainty is not None and local_hash is not None:
                # Specific key
                key = self._make_cache_key(min_certainty, local_hash)
                if key in self._cache and isinstance(self._cache[key], CacheEntry):
                    del self._cache[key]
                    self._stats.invalidations += 1
                    return 1
                return 0
            elif min_certainty is not None:
                # All entries with this certainty
                to_remove = [
                    k for k in self._cache.keys()
                    if k[0] == min_certainty and isinstance(self._cache[k], CacheEntry)
                ]
                for k in to_remove:
                    del self._cache[k]
                self._stats.invalidations += len(to_remove)
                return len(to_remove)
            else:
                # All entries
                count = sum(1 for v in self._cache.values() if isinstance(v, CacheEntry))
                self._cache.clear()
                self._stats.invalidations += count
                return count
    
    def _evict_oldest(self) -> None:
        """Evict the oldest cache entry (LRU). Only evicts CacheEntry, not sentinels."""
        if not self._cache:
            return
        
        # Find oldest CacheEntry
        oldest_key = None
        oldest_time = float('inf')
        
        for k, v in self._cache.items():
            if isinstance(v, CacheEntry) and v.created_at < oldest_time:
                oldest_time = v.created_at
                oldest_key = k
        
        if oldest_key is not None:
            del self._cache[oldest_key]
            self._stats.evictions += 1
    
    @property
    def size(self) -> int:
        """Number of completed entries in cache (excludes building sentinels)."""
        return sum(1 for v in self._cache.values() if isinstance(v, CacheEntry))
    
    @property
    def stats(self) -> 'CacheStats':
        """Get cache statistics."""
        return self._stats
    
    def get_info(self) -> Dict[str, Any]:
        """Get cache info for debugging/API."""
        with self._lock:
            entries = []
            building_count = 0
            
            for key, entry in self._cache.items():
                if isinstance(entry, _BuildingSentinel):
                    building_count += 1
                    continue
                    
                entries.append({
                    "minCertainty": key[0],
                    "localHash": key[1] or None,
                    "ageSeconds": round(entry.age_seconds(), 1),
                    "hits": entry.hits,
                    "expired": entry.is_expired(self._ttl),
                    "nodeCount": entry.value.node_count if entry.value else 0,
                    "edgeCount": entry.value.edge_count if entry.value else 0,
                })
            
            return {
                "ttlSeconds": self._ttl,
                "maxEntries": self._max_entries,
                "currentSize": len(entries),
                "buildingCount": building_count,
                "entries": entries,
                "stats": self._stats.to_dict(),
            }


@dataclass
class CacheStats:
    """Statistics for cache performance monitoring."""
    hits: int = 0
    misses: int = 0
    builds: int = 0
    evictions: int = 0
    expirations: int = 0
    invalidations: int = 0
    total_build_time: float = 0.0
    
    @property
    def hit_rate(self) -> float:
        """Cache hit rate (0-1)."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0
    
    @property
    def avg_build_time_ms(self) -> float:
        """Average graph build time in milliseconds."""
        return (self.total_build_time / self.builds * 1000) if self.builds > 0 else 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hitRate": round(self.hit_rate, 3),
            "builds": self.builds,
            "avgBuildTimeMs": round(self.avg_build_time_ms, 1),
            "evictions": self.evictions,
            "expirations": self.expirations,
            "invalidations": self.invalidations,
        }


# Module-level singleton cache
_default_cache: Optional[GraphCache] = None
_cache_lock = threading.Lock()


def get_graph_cache() -> GraphCache:
    """
    Get the default graph cache singleton.
    
    Creates the cache on first access.
    """
    global _default_cache
    
    if _default_cache is None:
        with _cache_lock:
            if _default_cache is None:
                _default_cache = GraphCache()
    
    return _default_cache


def get_cached_graph(
    conn_or_db: DBConnection,
    min_certainty: int = 5,
    local_hash: Optional[str] = None,
    force_rebuild: bool = False,
) -> Optional['MeshGraph']:
    """
    Convenience function to get graph from default cache.
    
    Args:
        conn_or_db: Database connection
        min_certainty: Minimum certainty for edges
        local_hash: Local node hash
        force_rebuild: Bypass cache
        
    Returns:
        Cached or freshly built MeshGraph
        
    Example:
        >>> graph = get_cached_graph(conn, min_certainty=5)
        >>> bridges = graph.find_bridges()
    """
    cache = get_graph_cache()
    return cache.get_or_build(
        conn_or_db,
        min_certainty=min_certainty,
        local_hash=local_hash,
        force_rebuild=force_rebuild,
    )


def invalidate_graph_cache(
    min_certainty: Optional[int] = None,
    local_hash: Optional[str] = None,
) -> int:
    """
    Invalidate graph cache entries.
    
    Args:
        min_certainty: Specific certainty to invalidate
        local_hash: Specific local_hash to invalidate
        
    Returns:
        Number of entries invalidated
    """
    cache = get_graph_cache()
    return cache.invalidate(min_certainty, local_hash)


def get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics and info."""
    cache = get_graph_cache()
    return cache.get_info()
