"""
Database Connection Manager for Analytics Module
=================================================

Provides centralized, thread-safe database connectivity for all analytics
operations. Ensures consistent connection configuration, error handling,
and resource management across the module.

Design Goals
------------
    1. **Single source of truth** for DB path and configuration
    2. **Thread safety** via connection-per-thread pattern
    3. **WAL mode** for better concurrent read/write performance
    4. **Consistent error handling** with proper logging
    5. **Resource cleanup** to prevent connection leaks

Connection Strategy
-------------------
SQLite connections are NOT thread-safe by default. This module provides:

    - get_connection(): Get a new connection for the calling thread
    - ConnectionContext: Context manager for automatic cleanup
    - execute_query(): Convenience wrapper for simple queries

WAL Mode
--------
Write-Ahead Logging (WAL) is enabled for all connections:
    - Readers don't block writers
    - Writers don't block readers
    - Better performance for read-heavy workloads (analytics)
    - Automatic checkpointing

Usage
-----
    from repeater.analytics.db import AnalyticsDB
    
    # Initialize once (typically in StorageCollector)
    db = AnalyticsDB(storage_dir / "repeater.db")
    
    # Use in any thread
    with db.connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT ...")
        results = cursor.fetchall()
    
    # Or use convenience method
    rows = db.execute_query("SELECT * FROM packets WHERE timestamp > ?", (cutoff,))

Thread Safety Notes
-------------------
    - Each thread should use its own connection (via connection() context)
    - Don't pass connection objects between threads
    - The AnalyticsDB instance itself is thread-safe to share

See Also
--------
    - worker.py: Uses AnalyticsDB for background processing
    - analytics_api.py: Uses AnalyticsDB for API queries
    - storage_collector.py: Initializes and owns AnalyticsDB instance
"""

import logging
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

# Type alias for row results - sqlite3.Row supports both index and key access
# When row_factory = sqlite3.Row, rows are sqlite3.Row objects (not dicts)
# Use row["column"] or row[0] syntax, or dict(row) for full dict conversion
RowType = sqlite3.Row

logger = logging.getLogger("Analytics.DB")

# SQLite configuration for analytics workload
SQLITE_PRAGMAS = {
    "journal_mode": "WAL",          # Write-Ahead Logging for concurrency
    "synchronous": "NORMAL",        # Balance safety and speed
    "cache_size": -64000,           # 64MB cache (negative = KB)
    "temp_store": "MEMORY",         # Temp tables in memory
    "mmap_size": 268435456,         # 256MB memory-mapped I/O
    "busy_timeout": 30000,          # 30s timeout for locked DB
}

# Connection pool settings (per-thread)
_thread_local = threading.local()


class AnalyticsDBError(Exception):
    """Base exception for analytics database errors."""
    pass


class ConnectionError(AnalyticsDBError):
    """Failed to establish database connection."""
    pass


class QueryError(AnalyticsDBError):
    """Database query failed."""
    pass


class AnalyticsDB:
    """
    Centralized database connection manager for analytics.
    
    Provides thread-safe access to the SQLite database with proper
    configuration for analytics workloads.
    
    Attributes:
        db_path: Path to the SQLite database file
        
    Example:
        >>> db = AnalyticsDB("/var/lib/pymc_repeater/repeater.db")
        >>> with db.connection() as conn:
        ...     cursor = conn.cursor()
        ...     cursor.execute("SELECT COUNT(*) FROM packets")
        ...     count = cursor.fetchone()[0]
    """
    
    def __init__(self, db_path: Union[str, Path]):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to SQLite database file. Will be created if
                     it doesn't exist (along with parent directories).
        """
        self.db_path = Path(db_path)
        self._ensure_db_exists()
        self._lock = threading.Lock()
        
        logger.info(f"AnalyticsDB initialized: {self.db_path}")
    
    def _ensure_db_exists(self) -> None:
        """Ensure database file and parent directories exist."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Touch the file if it doesn't exist
        if not self.db_path.exists():
            self.db_path.touch()
            logger.info(f"Created database file: {self.db_path}")
    
    def _configure_connection(self, conn: sqlite3.Connection) -> None:
        """Apply standard pragmas to a new connection."""
        for pragma, value in SQLITE_PRAGMAS.items():
            try:
                conn.execute(f"PRAGMA {pragma} = {value}")
            except sqlite3.Error as e:
                logger.warning(f"Failed to set PRAGMA {pragma}: {e}")
        
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        
        # Row factory for dict-like access (optional)
        conn.row_factory = sqlite3.Row
    
    def get_connection(self) -> sqlite3.Connection:
        """
        Get a database connection for the current thread.
        
        Creates a new connection configured with analytics-optimized
        pragmas. Caller is responsible for closing the connection.
        
        Returns:
            Configured SQLite connection
            
        Raises:
            ConnectionError: If connection cannot be established
            
        Note:
            Prefer using the connection() context manager instead.
        """
        try:
            conn = sqlite3.connect(
                str(self.db_path),
                timeout=30.0,
                check_same_thread=False,  # We manage thread safety ourselves
            )
            self._configure_connection(conn)
            return conn
            
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            raise ConnectionError(f"Cannot connect to {self.db_path}: {e}") from e
    
    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        """
        Context manager for database connections.
        
        Automatically handles connection cleanup and commit/rollback.
        
        Yields:
            Configured SQLite connection with row_factory=sqlite3.Row.
            Query results are sqlite3.Row objects supporting both:
            - Index access: row[0], row[1]
            - Key access: row["column_name"]
            - Dict conversion: dict(row)
            
        Example:
            >>> with db.connection() as conn:
            ...     cursor = conn.execute("SELECT name, value FROM config")
            ...     for row in cursor:
            ...         print(row["name"], row["value"])  # Key access
            ...         print(dict(row))  # Full dict if needed
        """
        conn = self.get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def execute_query(
        self,
        query: str,
        params: Tuple = (),
        fetch: str = "all",
    ) -> Union[List[sqlite3.Row], Optional[sqlite3.Row], int]:
        """
        Execute a query and return results.
        
        Convenience method for simple queries. For complex operations,
        use the connection() context manager directly.
        
        Args:
            query: SQL query string
            params: Query parameters (tuple)
            fetch: Result mode - "all", "one", or "rowcount"
            
        Returns:
            Query results based on fetch mode:
            - "all": List of rows
            - "one": Single row or None
            - "rowcount": Number of affected rows
            
        Raises:
            QueryError: If query execution fails
            
        Example:
            >>> rows = db.execute_query(
            ...     "SELECT * FROM packets WHERE timestamp > ?",
            ...     (cutoff,)
            ... )
        """
        try:
            with self.connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                
                if fetch == "all":
                    return cursor.fetchall()
                elif fetch == "one":
                    return cursor.fetchone()
                elif fetch == "rowcount":
                    return cursor.rowcount
                else:
                    raise ValueError(f"Invalid fetch mode: {fetch}")
                    
        except sqlite3.Error as e:
            logger.error(f"Query failed: {e}\nQuery: {query[:200]}")
            raise QueryError(f"Query execution failed: {e}") from e
    
    def execute_many(
        self,
        query: str,
        params_list: List[Tuple],
    ) -> int:
        """
        Execute a query with multiple parameter sets.
        
        Efficient for bulk inserts/updates.
        
        Args:
            query: SQL query string with placeholders
            params_list: List of parameter tuples
            
        Returns:
            Total number of affected rows
            
        Example:
            >>> db.execute_many(
            ...     "INSERT INTO edge_observations VALUES (?, ?, ?)",
            ...     [(a1, b1, c1), (a2, b2, c2), ...]
            ... )
        """
        try:
            with self.connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(query, params_list)
                return cursor.rowcount
                
        except sqlite3.Error as e:
            logger.error(f"Batch query failed: {e}")
            raise QueryError(f"Batch execution failed: {e}") from e
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        result = self.execute_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
            fetch="one",
        )
        return result is not None
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a table."""
        rows = self.execute_query(f"PRAGMA table_info({table_name})")
        return [
            {
                "cid": row[0],
                "name": row[1],
                "type": row[2],
                "notnull": bool(row[3]),
                "default": row[4],
                "pk": bool(row[5]),
            }
            for row in rows
        ]
    
    def vacuum(self) -> None:
        """
        Compact the database file.
        
        Reclaims space from deleted records. Should be run periodically
        (e.g., weekly) during maintenance windows.
        
        Note:
            VACUUM requires exclusive access and may take time on large DBs.
        """
        try:
            # VACUUM must run outside a transaction
            conn = self.get_connection()
            conn.isolation_level = None
            conn.execute("VACUUM")
            conn.close()
            logger.info("Database vacuum completed")
            
        except sqlite3.Error as e:
            logger.error(f"Vacuum failed: {e}")
    
    def checkpoint(self) -> None:
        """
        Force a WAL checkpoint.
        
        Writes WAL contents to the main database file. Usually automatic,
        but can be called manually to reduce WAL file size.
        """
        try:
            with self.connection() as conn:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                logger.debug("WAL checkpoint completed")
                
        except sqlite3.Error as e:
            logger.warning(f"Checkpoint failed: {e}")
    
    @property
    def path(self) -> str:
        """Get database path as string."""
        return str(self.db_path)


# Type alias for functions that accept either connection type
DBConnection = Union[sqlite3.Connection, 'AnalyticsDB']


@contextmanager
def ensure_connection(conn_or_db: DBConnection) -> Iterator[sqlite3.Connection]:
    """
    Context manager that normalizes connection argument.
    
    Allows functions to accept either a raw sqlite3.Connection or an
    AnalyticsDB instance, making the API more flexible.
    
    Args:
        conn_or_db: Either a sqlite3.Connection or AnalyticsDB instance
        
    Yields:
        sqlite3.Connection for use in the block
        
    Example:
        def my_query(conn_or_db):
            with ensure_connection(conn_or_db) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT ...")
    """
    if isinstance(conn_or_db, AnalyticsDB):
        with conn_or_db.connection() as conn:
            yield conn
    else:
        # Already a raw connection - don't manage it (caller's responsibility)
        yield conn_or_db


# TypeVar for decorator return type preservation
F = TypeVar('F', bound=Callable[..., Any])


def with_connection(func: F) -> F:
    """
    Decorator that normalizes the first 'conn' argument.
    
    Use this decorator for functions that take a connection as their first
    parameter. The decorator will ensure the connection is properly
    unwrapped if an AnalyticsDB instance is passed.
    
    Example:
        @with_connection
        def my_query(conn: DBConnection, param1: str) -> List:
            cursor = conn.cursor()
            cursor.execute("SELECT ...")
            return cursor.fetchall()
            
        # Can call with either:
        result = my_query(sqlite3_conn, "value")
        result = my_query(analytics_db, "value")
    """
    from functools import wraps
    
    @wraps(func)
    def wrapper(conn_or_db: DBConnection, *args: Any, **kwargs: Any) -> Any:
        with ensure_connection(conn_or_db) as conn:
            return func(conn, *args, **kwargs)
    
    return wrapper  # type: ignore[return-value]


# Module-level singleton for convenience (optional usage pattern)
_default_db: Optional[AnalyticsDB] = None


def init_db(db_path: Union[str, Path]) -> AnalyticsDB:
    """
    Initialize the default database instance.
    
    Args:
        db_path: Path to SQLite database
        
    Returns:
        The initialized AnalyticsDB instance
    """
    global _default_db
    _default_db = AnalyticsDB(db_path)
    return _default_db


def get_db() -> AnalyticsDB:
    """
    Get the default database instance.
    
    Raises:
        RuntimeError: If init_db() hasn't been called
    """
    if _default_db is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _default_db
