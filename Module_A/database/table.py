# Table Abstraction Layer
# Provides a clean database table interface wrapping the B+ tree index.
# Abstracts away low-level B+ tree details from user code.

from __future__ import annotations

from typing import Any

from .bplustree import BPlusTree


class Table:
    """Table abstraction backed by a B+ tree index.
    
    Provides a standard database table interface with columns abstracted as
    key-value records. All records are indexed by a primary key (integer).
    Supports insert, delete, update, search, range queries, and bulk retrieval.
    """

    def __init__(
        self,
        name: str,
        schema: dict[str, Any] | None = None,
        order: int = 4,
        search_key: str | None = None,
    ) -> None:
        """Initialize a table with specified name and B+ tree order.
        
        Args:
            name: Name of the table (for identification purposes).
            schema: Optional schema definition as {column: type|constraint-dict}.
            order: B+ tree order determining branching factor (default=4).
            search_key: Optional field used as index key for record-style inserts.
        """
        self.name = name  # Table identifier
        self.schema = schema
        self.search_key = search_key
        self.index = BPlusTree(order=order)  # B+ tree index for key-value storage
        self.data = self.index  # Compatibility alias used by template notebook

        # If schema is provided but search_key is omitted, use a sensible default.
        if self.schema is not None and self.search_key is None:
            if "id" in self.schema:
                self.search_key = "id"
            else:
                self.search_key = next(iter(self.schema.keys()), None)

    @staticmethod
    def _resolve_expected_type(constraint: Any) -> type | None:
        """Resolve expected Python type from schema constraint entry."""
        if isinstance(constraint, type):
            return constraint
        if isinstance(constraint, dict):
            type_spec = constraint.get("type")
            if isinstance(type_spec, type):
                return type_spec
            if isinstance(type_spec, str):
                type_map: dict[str, type] = {
                    "int": int,
                    "str": str,
                    "float": float,
                    "bool": bool,
                    "dict": dict,
                    "list": list,
                }
                return type_map.get(type_spec)
        return None

    def validate_record(self, record: object) -> None:
        """Validate a record against table schema (if schema is defined)."""
        if self.schema is None:
            return

        if not isinstance(record, dict):
            raise TypeError("Record must be a dictionary when schema is defined")

        # Validate each column in the schema against the record, enforcing required fields, type constraints, nullability, and allowed values as specified in the schema.
        for column, constraint in self.schema.items():
            required = True
            nullable = False
            allowed_values = None

            if isinstance(constraint, dict):
                required = constraint.get("required", not constraint.get("nullable", False))
                nullable = constraint.get("nullable", False)
                allowed_values = constraint.get("allowed")

            if column not in record:
                if required:
                    raise ValueError(f"Missing required column '{column}'")
                continue

            value = record[column]
            if value is None:
                if not nullable:
                    raise ValueError(f"Column '{column}' cannot be null")
                continue

            expected_type = self._resolve_expected_type(constraint)
            if expected_type is not None and not isinstance(value, expected_type):
                raise TypeError(
                    f"Column '{column}' expects {expected_type.__name__}, got {type(value).__name__}"
                )

            if allowed_values is not None and value not in allowed_values:
                raise ValueError(
                    f"Column '{column}' must be one of {allowed_values}, got '{value}'"
                )

    def _extract_index_key(self, record: dict[str, Any]) -> Any:
        """Extract B+ tree key from record using configured search_key."""
        if self.search_key is None:
            raise ValueError("search_key is required for record-only insert mode")
        if self.search_key not in record:
            raise ValueError(f"Record must include search key '{self.search_key}'")
        return record[self.search_key]

    def insert(self, *args: object) -> None:
        """Insert or update a record in the table.
        
        Time Complexity: O(log n)
        
        Args:
            Accepts one of:
            - insert(key, record): explicit key mode (backward compatible)
            - insert(record): record-only mode using configured search_key
        """
        # Validate arguments and determine key/record based on input pattern.
        if len(args) == 1: # Record-only mode: extract key from record using search_key
            record = args[0]
            if not isinstance(record, dict):
                raise TypeError("insert(record) requires record to be a dict")
            self.validate_record(record)
            key = self._extract_index_key(record)
        elif len(args) == 2: # Explicit key mode: use provided key and record
            key, record = args
            self.validate_record(record)

            # Guard against accidental mismatch between explicit key and record key.
            if isinstance(record, dict) and self.search_key is not None and self.search_key in record:
                if record[self.search_key] != key:
                    raise ValueError(
                        f"Explicit key '{key}' does not match record[{self.search_key!r}]={record[self.search_key]!r}"
                    )
        else:
            raise TypeError("insert expects either (record) or (key, record)")

        self.index.insert(key, record)

    def delete(self, key: int) -> bool:
        """Delete a record from the table by primary key.
        
        Time Complexity: O(log n)
        
        Args:
            key: The primary key to delete.
        
        Returns:
            True if record was deleted, False if not found.
        """
        # Delegate to B+ tree delete operation
        return self.index.delete(key)

    def update(self, key: int, new_record: object) -> bool:
        """Update the record associated with a key.
        
        Time Complexity: O(log n)
        
        Args:
            key: The primary key to update.
            new_record: The new record value.
        
        Returns:
            True if record existed and was updated, False if not found.
        """

        # Validate new record against schema before attempting update.
        self.validate_record(new_record)

        # Guard against accidental mismatch between update key and record key (if record is dict).
        if isinstance(new_record, dict) and self.search_key is not None and self.search_key in new_record:
            if new_record[self.search_key] != key:
                raise ValueError(
                    f"Update key '{key}' does not match record[{self.search_key!r}]={new_record[self.search_key]!r}"
                )

        return self.index.update(key, new_record)

    def get(self, record_id: object) -> object | None:
        """Template-compatible alias for select()."""
        return self.select(record_id)

    def select(self, key: int) -> object | None:
        """Retrieve a single record by primary key.
        
        Time Complexity: O(log n)
        
        Args:
            key: The primary key to search for.
        
        Returns:
            The record if found, None otherwise.
        """
        # Delegate to B+ tree search operation
        return self.index.search(key)

    def range_query(self, start_key: int, end_key: int) -> list[tuple[int, object]]:
        """Retrieve all records with keys in a range.
        
        Time Complexity: O(log n + k) where k is number of results.
        
        Args:
            start_key: Lower bound of range (inclusive).
            end_key: Upper bound of range (inclusive).
        
        Returns:
            List of (key, record) tuples for keys in range, sorted by key.
        """
        # Delegate to B+ tree range query operation
        return self.index.range_query(start_key, end_key)

    def all_records(self) -> list[tuple[int, object]]:
        """Retrieve all records from the table in sorted key order.
        
        Time Complexity: O(n)
        
        Returns:
            List of all (key, record) tuples sorted by key.
        """
        # Delegate to B+ tree get_all operation
        return self.index.get_all()

    def get_all(self) -> list[tuple[int, object]]:
        """Template-compatible alias for all_records()."""
        return self.all_records()
