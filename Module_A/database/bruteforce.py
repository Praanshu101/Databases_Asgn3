# Brute Force Database Implementation
# A simple baseline implementation using a Python list for storage.
# All operations perform linear scans, resulting in O(n) time complexity.
# Used for performance comparison against optimized B+ Tree implementation.

from __future__ import annotations


class BruteForceDB:
    """Simple baseline key-value store with linear scans for benchmarking.
    
    This class serves as a comparison point for B+ Tree performance.
    It uses a list for storage and performs exhaustive searches for all operations.
    Time complexity is O(n) for search, insert, delete, and range_query operations.
    """

    def __init__(self) -> None:
        """Initialize an empty brute force database.
        
        Attributes:
            _records: List of (key, value) tuples stored in insertion order.
        """
        self._records: list[tuple[int, object]] = []

    def insert(self, key: int, value: object) -> None:
        """Insert or update a key-value pair in the database.
        
        Time Complexity: O(n) - must search for existing key.
        
        Args:
            key: The integer key to insert.
            value: The value to associate with the key.
        """
        # Linear search to check if key already exists
        for i, (k, _) in enumerate(self._records):
            if k == key:
                # Key exists: update its value
                self._records[i] = (key, value)
                return
        
        # Key doesn't exist: append new record
        self._records.append((key, value))

    def search(self, key: int) -> object | None:
        """Search for a key and return its associated value.
        
        Time Complexity: O(n) - exhaustive linear search.
        
        Args:
            key: The integer key to search for.
        
        Returns:
            The value associated with the key, or None if not found.
        """
        # Linear scan through all records to find matching key
        for k, v in self._records:
            if k == key:
                return v
        return None

    def delete(self, key: int) -> bool:
        """Delete a key from the database.
        
        Time Complexity: O(n) - search + removal.
        
        Args:
            key: The integer key to delete.
        
        Returns:
            True if key was found and deleted, False if not found.
        """
        # Linear search to find the key
        for i, (k, _) in enumerate(self._records):
            if k == key:
                # Remove the record at index i
                self._records.pop(i)
                return True
        return False

    def update(self, key: int, value: object) -> bool:
        """Update the value associated with a key.
        
        Time Complexity: O(n) - linear search.
        
        Args:
            key: The integer key to update.
            value: The new value to associate with the key.
        
        Returns:
            True if key existed and was updated, False if key not found.
        """
        # Linear search to find the key
        for i, (k, _) in enumerate(self._records):
            if k == key:
                # Update the value at this index
                self._records[i] = (key, value)
                return True
        return False

    def range_query(self, start_key: int, end_key: int) -> list[tuple[int, object]]:
        """Retrieve all records with keys in range [start_key, end_key].
        
        Time Complexity: O(n) - must scan all records.
        
        Args:
            start_key: Lower bound of range (inclusive).
            end_key: Upper bound of range (inclusive).
        
        Returns:
            List of (key, value) tuples for keys in range, sorted by key.
        """
        # Linear scan collecting all records within range
        out = [(k, v) for k, v in self._records if start_key <= k <= end_key]
        # Sort results by key for consistent output
        out.sort(key=lambda x: x[0])
        return out

    def get_all(self) -> list[tuple[int, object]]:
        """Retrieve all records from the database.
        
        Time Complexity: O(n log n) - sorting operation dominates.
        
        Returns:
            List of all (key, value) tuples sorted by key.
        """
        # Return copy of all records, sorted by key
        return sorted(self._records, key=lambda x: x[0])
