# B+ Tree Implementation Module
# This module implements a self-balancing B+ tree data structure for efficient
# key-value storage and retrieval with O(log n) time complexity for all operations.
# Features include automatic node splitting/merging, range queries via linked leaves,
# and Graphviz visualization support.

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
from itertools import count

from graphviz import Digraph


@dataclass
class BPlusTreeNode:
    """Represents a single node in the B+ tree.
    
    Attributes:
        is_leaf: True if this node is a leaf node (stores values), False for internal nodes.
        keys: Sorted list of integer keys stored in this node.
        children: List of child node references (only relevant for internal nodes).
        values: List of values associated with keys (only used in leaf nodes).
        next: Pointer to the next leaf node (forms linked list for range queries).
    """
    is_leaf: bool = False # True for leaf nodes, False for internal nodes
    keys: list[int] = field(default_factory=list) # Sorted keys in the node (max length = order-1)
    children: list[BPlusTreeNode] = field(default_factory=list) # Child pointers for internal nodes (length = len(keys) + 1)
    values: list[object] = field(default_factory=list) # Values for leaf nodes (aligned with keys)
    next: BPlusTreeNode | None = None # Pointer to next leaf node (for linked list of leaves)


class BPlusTree:
    """In-memory B+ tree supporting insert, delete, exact/range query, and visualization.
    
    A B+ tree is a self-balancing tree structure that maintains sorted order and allows
    searches, sequential access, insertions, and deletions in logarithmic time.
    All values are stored in leaf nodes, internal nodes only store separator keys.
    """

    def __init__(self, order: int = 4) -> None:
        """Initialize a B+ tree with specified order (branching factor).
        
        Args:
            order: The branching factor of the tree (default=4). Must be >= 3.
                   Determines max keys per node: order-1
                   Determines min keys per node: ceil(order/2)-1
        
        Raises:
            ValueError: If order is less than 3.
        """
        if order < 3:
            raise ValueError("B+ tree order must be at least 3")
        self.order = order
        self.max_keys = order - 1  # Maximum number of keys a node can hold
        self.min_keys = (order + 1) // 2 - 1  # Minimum keys required (except root)
        self.root = BPlusTreeNode(is_leaf=True)  # Start with single empty leaf node

    def search(self, key: int) -> object | None:
        """Search for a key in the tree and return its associated value.
        
        Time Complexity: O(log n) where n is the number of keys.
        
        Args:
            key: The integer key to search for.
        
        Returns:
            The value associated with the key if found, None otherwise.
        """
        # Traverse tree to find the leaf node that should contain the key
        node = self._find_leaf(key)
        
        # Use binary search on the leaf node's keys to find exact position
        idx = bisect_left(node.keys, key)
        
        # Check if the key exists at the found position
        if idx < len(node.keys) and node.keys[idx] == key:
            return node.values[idx]
        return None

    def insert(self, key: int, value: object) -> None:
        """Insert a key-value pair into the tree. Updates existing key's value if present.
        
        Time Complexity: O(log n) with automatic node splitting.
        
        Args:
            key: The integer key to insert.
            value: The value to associate with the key.
        """
        # If root is full, create new root and split old root (tree grows upward)
        if len(self.root.keys) == self.max_keys:
            new_root = BPlusTreeNode(is_leaf=False, children=[self.root])
            self._split_child(new_root, 0)
            self.root = new_root
        
        # Recursively insert into non-full root
        self._insert_non_full(self.root, key, value)

    def _insert_non_full(self, node: BPlusTreeNode, key: int, value: object) -> None:
        """Recursive helper to insert into a node that is not full.
        
        Args:
            node: The node to insert into (guaranteed not full).
            key: The key to insert.
            value: The value to associate with the key.
        """
        if node.is_leaf:
            # Leaf node: directly insert key-value pair in sorted order
            idx = bisect_left(node.keys, key)
            
            # If key already exists, update its value
            if idx < len(node.keys) and node.keys[idx] == key:
                node.values[idx] = value
                return
            
            # Insert new key-value pair at correct position
            node.keys.insert(idx, key)
            node.values.insert(idx, value)
            return

        # Internal node: recursively insert into appropriate child
        idx = bisect_right(node.keys, key)
        
        # Check if child will be full after insertion
        if len(node.children[idx].keys) == self.max_keys:
            # Split child before insertion to prevent overflow
            self._split_child(node, idx)
            # After split, check which side the key belongs to
            if key >= node.keys[idx]:
                idx += 1
        
        self._insert_non_full(node.children[idx], key, value)

    def _split_child(self, parent: BPlusTreeNode, index: int) -> None:
        """Split a full child node into two nodes and move separator key to parent.
        
        For leaf nodes: Copy middle key to parent, create linked list connection.
        For internal nodes: Promote middle key to parent, redistribute children.
        
        Args:
            parent: The parent node containing the full child.
            index: Index of the child to split in parent's children list.
        """
        child = parent.children[index]
        mid = len(child.keys) // 2  # Find midpoint for split
        
        # Create new node to hold second half
        new_node = BPlusTreeNode(is_leaf=child.is_leaf)
        
        if child.is_leaf:
            # Leaf split: copy upper half to new node, keep lower half in original
            new_node.keys = child.keys[mid:]
            new_node.values = child.values[mid:]
            child.keys = child.keys[:mid]
            child.values = child.values[:mid]
            
            # Maintain linked list of leaves (critical for range queries)
            new_node.next = child.next
            child.next = new_node
            
            # Copy separator key to parent (in B+ trees, leaf keys are also in parents)
            parent.keys.insert(index, new_node.keys[0])
            parent.children.insert(index + 1, new_node)
            return

        # Internal node split: promote middle key to parent
        promote_key = child.keys[mid]
        new_node.keys = child.keys[mid + 1:]  # Upper keys to new node
        new_node.children = child.children[mid + 1:]  # Upper children to new node
        child.keys = child.keys[:mid]  # Lower keys stay in original
        child.children = child.children[: mid + 1]  # Lower children stay original

        # Insert promoted key and new child reference into parent
        parent.keys.insert(index, promote_key)
        parent.children.insert(index + 1, new_node)

    def delete(self, key: int) -> bool:
        """Delete a key from the tree.
        
        Time Complexity: O(log n) with automatic rebalancing.
        
        Args:
            key: The integer key to delete.
        
        Returns:
            True if key was deleted, False if key was not found.
        """
        deleted = self._delete(self.root, key)
        
        # If root becomes empty after deletion, move up its only child as new root
        if not self.root.is_leaf and len(self.root.keys) == 0:
            self.root = self.root.children[0]
        
        return deleted

    def _delete(self, node: BPlusTreeNode, key: int) -> bool:
        """Recursive helper to delete a key from a node or its children.
        
        Args:
            node: Current node being processed.
            key: The key to delete.
        
        Returns:
            True if deletion succeeded, False if key not found.
        """
        if node.is_leaf:
            # Leaf: directly remove key if it exists
            idx = bisect_left(node.keys, key)
            if idx >= len(node.keys) or node.keys[idx] != key:
                return False  # Key not found
            
            # Remove key and associated value
            node.keys.pop(idx)
            node.values.pop(idx)
            return True

        # Internal node: find child that should contain the key
        idx = bisect_right(node.keys, key)
        child = node.children[idx]

        # Ensure child has enough keys before deletion (prevents underflow)
        if len(child.keys) == self.min_keys:
            self._fill_child(node, idx)
            idx = min(idx, len(node.children) - 1)

        # Recursively delete from appropriate child
        deleted = self._delete(node.children[idx], key)

        # Update separator keys to reflect leaf node changes
        for i in range(len(node.keys)):
            if node.children[i + 1].keys:
                node.keys[i] = node.children[i + 1].keys[0]

        return deleted

    def _fill_child(self, node: BPlusTreeNode, index: int) -> None:
        """Ensure child at index has at least min_keys by borrowing or merging.
        
        Strategy: Try to borrow from siblings first; if they don't have extra keys, merge.
        
        Args:
            node: The parent node.
            index: Index of the child that needs filling.
        """
        # Try to borrow from left sibling if it has extra keys
        if index > 0 and len(node.children[index - 1].keys) > self.min_keys:
            self._borrow_from_prev(node, index)
            return
        
        # Try to borrow from right sibling if it has extra keys
        if index < len(node.children) - 1 and len(node.children[index + 1].keys) > self.min_keys:
            self._borrow_from_next(node, index)
            return

        # Both siblings have minimum keys: merge with one of them
        if index < len(node.children) - 1:
            self._merge(node, index)
        else:
            self._merge(node, index - 1)

    def _borrow_from_prev(self, node: BPlusTreeNode, index: int) -> None:
        """Borrow one key from left sibling to prevent underflow of child.
        
        Args:
            node: The parent node.
            index: Index of child that needs a key.
        """
        child = node.children[index]
        sibling = node.children[index - 1]

        if child.is_leaf:
            # Leaf: move last key from sibling to first position of child
            child.keys.insert(0, sibling.keys.pop())
            child.values.insert(0, sibling.values.pop())
            # Update separator key in parent to reflect the move
            node.keys[index - 1] = child.keys[0]
            return

        # Internal: borrow via separator key rotation
        child.keys.insert(0, node.keys[index - 1])  # Separator from parent to child
        child.children.insert(0, sibling.children.pop())  # Child pointer from sibling
        node.keys[index - 1] = sibling.keys.pop()  # Sibling's last key becomes new separator

    def _borrow_from_next(self, node: BPlusTreeNode, index: int) -> None:
        """Borrow one key from right sibling to prevent underflow of child.
        
        Args:
            node: The parent node.
            index: Index of child that needs a key.
        """
        child = node.children[index]
        sibling = node.children[index + 1]

        if child.is_leaf:
            # Leaf: move first key from sibling to end of child
            child.keys.append(sibling.keys.pop(0))
            child.values.append(sibling.values.pop(0))
            # Update separator key in parent to reflect the move
            if sibling.keys:
                node.keys[index] = sibling.keys[0]
            return

        # Internal: borrow via separator key rotation
        child.keys.append(node.keys[index])  # Separator from parent to child
        child.children.append(sibling.children.pop(0))  # Child pointer from sibling
        node.keys[index] = sibling.keys.pop(0)  # Sibling's first key becomes new separator

    def _merge(self, node: BPlusTreeNode, index: int) -> None:
        """Merge child at index with its right sibling, pulling separator from parent.
        
        Args:
            node: The parent node.
            index: Index of left child to merge (will merge with right sibling).
        """
        left = node.children[index]
        right = node.children[index + 1]

        if left.is_leaf:
            # Leaf merge: combine keys/values and maintain leaf linkage
            left.keys.extend(right.keys)
            left.values.extend(right.values)
            left.next = right.next  # Update linked list pointer
            
            # Remove separator key and child reference from parent
            node.keys.pop(index)
            node.children.pop(index + 1)
            return

        # Internal node merge: include separator key from parent
        left.keys.append(node.keys.pop(index))  # Separator becomes last key of left
        left.keys.extend(right.keys)  # Add all of right's keys
        left.children.extend(right.children)  # Add all of right's children
        node.children.pop(index + 1)  # Remove right child reference

    def update(self, key: int, new_value: object) -> bool:
        """Update the value associated with an existing key.
        
        Time Complexity: O(log n)
        
        Args:
            key: The key whose value should be updated.
            new_value: The new value to associate with the key.
        
        Returns:
            True if key existed and was updated, False if key not found.
        """
        # Find leaf node that should contain the key
        node = self._find_leaf(key)
        
        # Binary search for the key in the leaf
        idx = bisect_left(node.keys, key)
        
        # Update value if key exists
        if idx < len(node.keys) and node.keys[idx] == key:
            node.values[idx] = new_value
            return True
        
        return False

    def range_query(self, start_key: int, end_key: int) -> list[tuple[int, object]]:
        """Retrieve all key-value pairs within a given range [start_key, end_key].
        
        Uses the linked leaf structure for efficient sequential access.
        Time Complexity: O(log n + k) where k is the number of results.
        
        Args:
            start_key: Lower bound of range (inclusive).
            end_key: Upper bound of range (inclusive).
        
        Returns:
            List of (key, value) tuples for all keys in range, sorted by key.
        """
        # Normalize range if bounds are reversed
        if start_key > end_key:
            start_key, end_key = end_key, start_key

        # Binary search to find starting leaf node
        node = self._find_leaf(start_key)
        result: list[tuple[int, object]] = []

        # Walk through linked leaves, collecting keys in range
        while node:
            for i, k in enumerate(node.keys):
                # Stop when we exceed range (keys are sorted)
                if k > end_key:
                    return result
                
                # Collect keys within range
                if start_key <= k <= end_key:
                    result.append((k, node.values[i]))
            
            # Move to next leaf node in linked list
            node = node.next

        return result

    def get_all(self) -> list[tuple[int, object]]:
        """Retrieve all key-value pairs in the tree in sorted order.
        
        Uses in-order traversal via the linked leaf structure.
        Time Complexity: O(n) where n is the number of keys.
        
        Returns:
            List of all (key, value) tuples in sorted key order.
        """
        # Navigate to leftmost leaf node
        node = self.root
        while not node.is_leaf:
            node = node.children[0]

        # Collect all key-value pairs by walking linked leaves
        out: list[tuple[int, object]] = []
        while node:
            # Pair keys with values and add to result
            out.extend(zip(node.keys, node.values))
            # Move to next leaf in linked list
            node = node.next
        
        return out

    def visualize_tree(self) -> Digraph:
        """Generate a Graphviz representation of the tree structure.
        
        Returns:
            A Graphviz Digraph object that can be rendered to PNG/PDF/SVG.
            Shows internal nodes and leaves with linkages between them.
        """
        # Initialize Graphviz directed graph object
        dot = Digraph(comment="B+ Tree")
        
        dot.attr(rankdir="TB", splines="polyline", nodesep="0.35", ranksep="0.65") 
        dot.attr("node", fontname="Times-Roman", fontsize="10")
        dot.attr("edge", arrowsize="0.7")
        id_gen = count()  # Generator for unique node IDs
        node_ids: dict[int, str] = {}  # Map Python object id to Graphviz node ID

        # Recursively add nodes and build the graph structure
        self._add_nodes(dot, self.root, node_ids, id_gen)
        self._add_edges(dot, self.root, node_ids)
        self._add_leaf_linkage(dot, node_ids)
        
        return dot

    def _add_nodes(
        self,
        dot: Digraph,
        node: BPlusTreeNode,
        node_ids: dict[int, str],
        id_gen,
    ) -> None:
        """Recursively add nodes to Graphviz graph.
        
        Args:
            dot: The Graphviz Digraph object to add nodes to.
            node: Current node being processed.
            node_ids: Dictionary mapping Python object IDs to Graphviz node IDs.
            id_gen: Generator for unique Graphviz node IDs.
        """
        # Generate unique ID for this node
        nid = f"n{next(id_gen)}"
        node_ids[id(node)] = nid

        if node.is_leaf:
            # Leaf nodes: show as boxes with their keys
            label = " | ".join(str(k) for k in node.keys) if node.keys else "<empty>"
            dot.node(nid, f"Leaf: {label}", shape="box", style="rounded,filled", fillcolor="lightyellow")
        else:
            # Internal nodes: show as ellipses with separator keys
            label = " | ".join(str(k) for k in node.keys) if node.keys else "<root>"
            dot.node(nid, f"Internal: {label}", shape="ellipse", style="filled", fillcolor="aliceblue")
            
            # Recursively add all children
            for child in node.children:
                self._add_nodes(dot, child, node_ids, id_gen)

    def _add_edges(self, dot: Digraph, node: BPlusTreeNode, node_ids: dict[int, str]) -> None:
        """Recursively add edges to Graphviz graph.
        
        Adds parent-child edges for tree structure and dashed blue edges between
        consecutive leaf nodes to show the linked list linkage.
        
        Args:
            dot: The Graphviz Digraph object to add edges to.
            node: Current node being processed.
            node_ids: Dictionary mapping Python object IDs to Graphviz node IDs.
        """
        if node.is_leaf:
            return

        # Internal node: add edges to all children and recurse
        for child in node.children:
            dot.edge(node_ids[id(node)], node_ids[id(child)])
            self._add_edges(dot, child, node_ids)

    def _add_leaf_linkage(self, dot: Digraph, node_ids: dict[int, str]) -> None:
        """Render linked-list edges between leaves and keep leaves on one rank."""
        leaves = self._collect_leaves()
        if not leaves:
            return

        # Keep leaves horizontally aligned to make linkage easy to inspect.
        with dot.subgraph() as same_rank:
            same_rank.attr(rank="same")
            for leaf in leaves:
                same_rank.node(node_ids[id(leaf)])

        for i in range(len(leaves) - 1):
            src = node_ids[id(leaves[i])]
            dst = node_ids[id(leaves[i + 1])]
            dot.edge(
                src,
                dst,
                style="dashed",
                color="blue",
                penwidth="1.2",
                constraint="false",
                label="next" if i == 0 else "",
            )

    def _collect_leaves(self) -> list[BPlusTreeNode]:
        """Collect all leaves from left to right using next pointers."""
        node = self.root
        while not node.is_leaf:
            node = node.children[0]

        leaves: list[BPlusTreeNode] = []
        while node is not None:
            leaves.append(node)
            node = node.next

        return leaves

    def _find_leaf(self, key: int) -> BPlusTreeNode:
        """Navigate from root to the leaf node that should contain the key.
        
        Time Complexity: O(log n)
        
        Args:
            key: The key being searched for (used to guide traversal).
        
        Returns:
            The leaf node that should contain the key (or where it should be inserted).
        """
        node = self.root
        
        # Move down tree until reaching a leaf
        while not node.is_leaf:
            # Binary search to find which child to follow
            # bisect_right returns position where key would be inserted
            idx = bisect_right(node.keys, key)
            node = node.children[idx]
        
        return node
