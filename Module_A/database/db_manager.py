"""Database Manager and Performance Analysis Module.

This module provides high-level database management functionality and comprehensive
performance benchmarking capabilities. It includes:

- DatabaseManager: Multi-table management system
- PerformanceAnalyzer: Automated benchmarking framework with performance metrics
- BenchmarkResult: Data container for benchmark results

The PerformanceAnalyzer compares B+ Tree operations against a BruteForce baseline
to demonstrate performance improvements across insert, delete, search, and range
query operations.
"""

from dataclasses import dataclass
from typing import Any, Dict, List

import sys
import time
import random
import types

from .bplustree import BPlusTree
from .bruteforce import BruteForceDB
from .table import Table


@dataclass # Using dataclass for convenient storage of benchmark results
class BenchmarkResult:
    """Container for individual benchmark results.
    
    Attributes:
        size: Number of records in the dataset
        insert_bptree_s: Time (seconds) for B+ Tree insertions
        insert_bruteforce_s: Time (seconds) for BruteForce insertions
        search_bptree_s: Time (seconds) for B+ Tree searches
        search_bruteforce_s: Time (seconds) for BruteForce searches
        delete_bptree_s: Time (seconds) for B+ Tree deletions
        delete_bruteforce_s: Time (seconds) for BruteForce deletions
        range_bptree_s: Time (seconds) for B+ Tree range queries
        range_bruteforce_s: Time (seconds) for BruteForce range queries
        mem_bptree_kb: Peak memory usage (KB) for B+ Tree structure
        mem_bruteforce_kb: Peak memory usage (KB) for BruteForce structure
    """
    size: int
    insert_bptree_s: float
    insert_bruteforce_s: float
    search_bptree_s: float
    search_bruteforce_s: float
    delete_bptree_s: float
    delete_bruteforce_s: float
    range_bptree_s: float
    range_bruteforce_s: float
    mem_bptree_kb: float
    mem_bruteforce_kb: float


class DatabaseManager:
    """Multi-table database management system.
    
    Manages creation, retrieval, and deletion of multiple Table instances
    with B+ Tree backing. Each table is indexed by a unique name.
    
    Attributes:
        _tables: Dictionary mapping table names to Table instances
    """
    
    def __init__(self):
        """Initialize an empty database manager."""
        self._default_db = "__default__"  # Backward-compatible default namespace
        self.databases: Dict[str, Dict[str, Table]] = {self._default_db: {}}
        self._tables = self.databases[self._default_db]  # Legacy alias used elsewhere

    def create_database(self, db_name: str) -> None:
        """Create a new logical database namespace.

        Time Complexity: O(1)

        Args:
            db_name: Name of database namespace to create

        Raises:
            ValueError: If namespace already exists
        """
        if db_name in self.databases:
            raise ValueError(f"Database '{db_name}' already exists")
        # Initialize empty table registry for this database
        self.databases[db_name] = {}

    def delete_database(self, db_name: str) -> None:
        """Delete an existing logical database namespace.

        Time Complexity: O(1)

        Args:
            db_name: Name of database namespace to delete

        Raises:
            ValueError: If attempting to delete default database
            KeyError: If database does not exist
        """
        if db_name == self._default_db:
            raise ValueError("Default database cannot be deleted")
        if db_name not in self.databases:
            raise KeyError(f"Database '{db_name}' does not exist")
        # Remove entire namespace and all its table references
        del self.databases[db_name]

    def list_databases(self) -> List[str]:
        """Return names of all available databases.

        Time Complexity: O(n) where n = number of databases
        """
        return list(self.databases.keys())

    def _get_db_tables(self, db_name: str) -> Dict[str, Table]:
        """Fetch table dictionary for a given database namespace."""
        if db_name not in self.databases:
            raise KeyError(f"Database '{db_name}' does not exist")
        return self.databases[db_name]
    
    def create_table(self, *args: Any, **kwargs: Any) -> Table:
        """Create a new table with B+ Tree backing.
        
        Supports two forms for compatibility:
        1) create_table(name, order=4, schema=None, search_key=None)
        2) create_table(db_name, table_name, schema, order=8, search_key=None)
        """
        # MODE 1: Template-style call with explicit database and schema
        if len(args) >= 3:
            db_name = args[0]
            table_name = args[1]
            schema = args[2]
            order = kwargs.get("order", 8)
            search_key = kwargs.get("search_key")
        # MODE 2: Existing assignment-style call on default database
        elif len(args) >= 1:
            db_name = kwargs.get("db_name", self._default_db)
            table_name = args[0]

            # Support create_table(name, schema_dict, order=...)
            if len(args) >= 2 and isinstance(args[1], dict):
                schema = args[1]
                order = kwargs.get("order", 4)
            # Support create_table(name, order_int)
            elif len(args) >= 2 and isinstance(args[1], int):
                schema = kwargs.get("schema")
                order = args[1]
            # Support create_table(name, order=..., schema=...)
            else:
                schema = kwargs.get("schema")
                order = kwargs.get("order", 4)

            search_key = kwargs.get("search_key")
        else:
            raise TypeError("create_table requires at least one positional argument")

        tables = self._get_db_tables(db_name)
        if table_name in tables:
            raise ValueError(f"Table '{table_name}' already exists in database '{db_name}'")

        # Construct schema-aware table and register it in selected namespace
        table = Table(table_name, schema=schema, order=order, search_key=search_key)
        tables[table_name] = table

        # Keep old direct access in sync for default database users.
        if db_name == self._default_db:
            self._tables = tables

        return table
    
    def get_table(self, *args: str) -> Table:
        """Retrieve an existing table by name.

        Supports:
        - get_table(name)
        - get_table(db_name, table_name)
        """
        # Resolve overloaded signature into (db_name, table_name)
        if len(args) == 1:
            db_name = self._default_db
            table_name = args[0]
        elif len(args) == 2:
            db_name, table_name = args
        else:
            raise TypeError("get_table expects (name) or (db_name, table_name)")

        tables = self._get_db_tables(db_name)
        if table_name not in tables:
            raise KeyError(f"Table '{table_name}' does not exist in database '{db_name}'")
        return tables[table_name]
    
    def drop_table(self, *args: str) -> None:
        """Delete a table from the database.

        Supports:
        - drop_table(name)
        - drop_table(db_name, table_name)
        """
        # Resolve overloaded signature into (db_name, table_name)
        if len(args) == 1:
            db_name = self._default_db
            table_name = args[0]
        elif len(args) == 2:
            db_name, table_name = args
        else:
            raise TypeError("drop_table expects (name) or (db_name, table_name)")

        tables = self._get_db_tables(db_name)
        if table_name not in tables:
            raise KeyError(f"Table '{table_name}' does not exist in database '{db_name}'")

        # Remove table registration (object is garbage-collected when unreferenced)
        del tables[table_name]

    def delete_table(self, db_name: str, table_name: str) -> None:
        """Template-compatible alias for dropping a table by database and name."""
        self.drop_table(db_name, table_name)
    
    def list_tables(self, db_name: str | None = None) -> List[str]:
        """Get list of all table names.

        Args:
            db_name: Optional database name. Uses default database when omitted.
        """
        selected_db = db_name or self._default_db
        tables = self._get_db_tables(selected_db)
        return list(tables.keys())


class PerformanceAnalyzer:
    """Benchmarking framework for comparing B+ Tree vs BruteForce implementations.
    
    Conducts systematic performance tests across multiple dataset sizes and
    measures execution time and memory usage for all major operations.
    """
    
    @staticmethod
    def benchmark(
        dataset_sizes: List[int],
        seed: int = 42,
        order: int = 4,
        search_count: int = 200,
        delete_count: int = 200,
        range_query_count: int = 100,
        key_space_multiplier: int = 20,
    ) -> List[BenchmarkResult]:
        """Execute comprehensive performance benchmark suite.
        
        Compares B+ Tree and BruteForce implementations across insert, search,
        delete, and range query operations. Uses consistent random data across
        both implementations for fair comparison.
        
        Time Complexity: O(n * log(n)) for B+ Tree, O(n^2) for BruteForce
        
        Args:
            dataset_sizes: List of dataset sizes to benchmark (e.g., [100, 500, 1000])
            seed: Random seed for reproducible data generation (default 42)
            order: B+ Tree order (default 4)
            search_count: Number of random search keys per dataset size (default 200)
            delete_count: Number of random delete keys per dataset size (default 200)
            range_query_count: Number of random range queries per dataset size (default 100)
            key_space_multiplier: Key universe size relative to n for random key generation
        
        Returns:
            List of BenchmarkResult objects, one per dataset size
        """
        results: List[BenchmarkResult] = []
        
        # Iterate through each dataset size
        for index, n in enumerate(dataset_sizes):
            # Use a dedicated RNG per dataset size for reproducibility with independence.
            rng = random.Random(seed + index)

            # DATA GENERATION PHASE
            # Generate n unique random keys from a larger key universe.
            key_space_size = max(n * key_space_multiplier, n + 1)
            key_space = list(range(100, 100 + key_space_size))
            keys = rng.sample(key_space, n)
            values = [f"value_{k}" for k in keys]

            # Select random key sets for search and delete operations.
            num_search = max(1, min(search_count, len(key_space)))
            num_delete = max(1, min(delete_count, len(keys)))
            search_keys = rng.sample(key_space, num_search)
            delete_keys = rng.sample(keys, num_delete)

            # Generate random ranges to benchmark range-query behavior.
            num_ranges = max(1, range_query_count)
            range_bounds: list[tuple[int, int]] = []
            for _ in range(num_ranges):
                a, b = rng.sample(key_space, 2)
                lo, hi = (a, b) if a <= b else (b, a)
                range_bounds.append((lo, hi))
            
            # TABLE INITIALIZATION 
            # Create B+ Tree-backed table and BruteForce baseline
            table = Table("benchmark", order=order)
            brute = BruteForceDB()
            
            # INSERTION BENCHMARK 
            # B+ Tree: Insert all (key, value) pairs sequentially
            t0 = time.perf_counter()
            for k, v in zip(keys, values):
                table.insert(k, v)
            insert_bptree = time.perf_counter() - t0
            
            # BruteForce: Insert same keys and values in same order
            t0 = time.perf_counter()
            for k, v in zip(keys, values):
                brute.insert(k, v)
            insert_bruteforce = time.perf_counter() - t0
            
            # SEARCH BENCHMARK 
            # B+ Tree: Search for 20% of randomly selected keys
            t0 = time.perf_counter()
            for k in search_keys:
                table.select(k)
            search_bptree = time.perf_counter() - t0
            
            # BruteForce: Same search operations
            t0 = time.perf_counter()
            for k in search_keys:
                brute.search(k)
            search_bruteforce = time.perf_counter() - t0
            
            # RANGE QUERY BENCHMARK
            # B+ Tree: execute many random range queries
            t0 = time.perf_counter()
            for lo, hi in range_bounds:
                table.range_query(lo, hi)
            range_bptree = time.perf_counter() - t0

            # BruteForce: Same range query
            t0 = time.perf_counter()
            for lo, hi in range_bounds:
                brute.range_query(lo, hi)
            range_bruteforce = time.perf_counter() - t0
            
            # DELETION BENCHMARK 
            # B+ Tree: Delete 20% of randomly selected keys
            t0 = time.perf_counter()
            for k in delete_keys:
                table.delete(k)
            delete_bptree = time.perf_counter() - t0
            
            # BruteForce: Delete same keys
            t0 = time.perf_counter()
            for k in delete_keys:
                brute.delete(k)
            delete_bruteforce = time.perf_counter() - t0
            
            # MEMORY USAGE BENCHMARK 
            # Measure peak memory consumption for each implementation
            mem_bptree = PerformanceAnalyzer._measure_memory_kb(table)
            mem_bruteforce = PerformanceAnalyzer._measure_memory_kb(brute)
            
            # RESULT STORAGE 
            # Store all metrics in BenchmarkResult object
            results.append(
                BenchmarkResult(
                    size=n,
                    insert_bptree_s=insert_bptree,
                    insert_bruteforce_s=insert_bruteforce,
                    search_bptree_s=search_bptree,
                    search_bruteforce_s=search_bruteforce,
                    delete_bptree_s=delete_bptree,
                    delete_bruteforce_s=delete_bruteforce,
                    range_bptree_s=range_bptree,
                    range_bruteforce_s=range_bruteforce,
                    mem_bptree_kb=mem_bptree,
                    mem_bruteforce_kb=mem_bruteforce,
                )
            )
        
        return results
    
    @staticmethod
    def _measure_memory_kb(obj: object) -> float:
        """Estimate deep memory usage of an object in kilobytes.

        The previous implementation used ``tracemalloc`` around ``repr(obj)``,
        which mostly measured temporary allocation noise and could report very
        similar values across different structures. This implementation walks the
        object graph and sums ``sys.getsizeof`` recursively for a retained-size
        estimate.

        Time Complexity: O(n) where n = number of reachable Python objects

        Args:
            obj: Object whose memory usage should be measured.

        Returns:
            Estimated deep memory usage in kilobytes.
        """

        seen: set[int] = set()
        total_bytes = 0
        stack: list[object] = [obj]

        # Walk the object graph using a stack to avoid recursion depth issues.
        while stack:
            value = stack.pop()
            obj_id = id(value)
            if obj_id in seen:
                continue
            seen.add(obj_id)

            total_bytes += sys.getsizeof(value)

            # Avoid descending into interpreter/runtime metadata objects.
            if isinstance(
                value,
                (
                    type,
                    types.ModuleType,
                    types.FunctionType,
                    types.BuiltinFunctionType,
                    types.MethodType,
                    types.CodeType,
                ),
            ):
                continue
            
            # Recursively add contained objects for common container types and user-defined classes.
            if isinstance(value, dict):
                stack.extend(value.keys())
                stack.extend(value.values())
            elif isinstance(value, (list, tuple, set, frozenset)):
                stack.extend(value)
            elif hasattr(value, "__dict__"):
                stack.append(vars(value))
            elif hasattr(value, "__slots__"):
                for slot in value.__slots__:
                    if hasattr(value, slot):
                        stack.append(getattr(value, slot))

        return total_bytes / 1024.0 # Convert bytes to kilobytes
