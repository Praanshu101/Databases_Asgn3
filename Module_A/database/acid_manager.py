from __future__ import annotations

import json
import os
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .db_manager import DatabaseManager


@dataclass
class TxOperation:
    """Represents a deferred write operation within a transaction."""

    op: str
    table: str
    key: Any
    value: Any = None


@dataclass
class TransactionContext:
    """Holds state for one active transaction."""

    txid: str
    operations: list[TxOperation] = field(default_factory=list)


class ACIDTransactionManager:
    """Transaction manager with WAL, crash recovery, and serialized isolation.

    Design choices:
    - Deferred updates: writes are staged in-memory and applied on COMMIT.
    - Write-ahead logging: every operation is persisted before data application.
    - Serialized commit path: one transaction at a time for simple isolation.
    - Snapshot checkpoints: committed state is stored to disk for durability.
    """

    def __init__(
        self,
        db_manager: DatabaseManager,
        storage_dir: str = "data",
        db_name: str = "__default__",
    ) -> None:
        self.db_manager = db_manager
        self.db_name = db_name
        self._lock = threading.RLock()
        self._tx_gate = threading.Lock()
        self._gated_txs: set[str] = set()
        self._active: dict[str, TransactionContext] = {}
        self._lsn = 0

        self.storage_path = Path(storage_dir)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.wal_path = self.storage_path / "wal.log"
        self.snapshot_path = self.storage_path / "snapshot.json"

        self._load_snapshot()
        self.recover()

    def begin(self) -> str:
        """Begin a new transaction and return transaction id."""
        # Serialize full transaction lifetime (BEGIN -> COMMIT/ROLLBACK).
        self._tx_gate.acquire()
        with self._lock:
            try:
                txid = uuid.uuid4().hex
                self._active[txid] = TransactionContext(txid=txid)
                self._gated_txs.add(txid)
                self._append_wal({"type": "BEGIN", "txid": txid})
                return txid
            except Exception:
                self._tx_gate.release()
                raise

    def insert(self, txid: str, table: str, key: Any, value: Any) -> None:
        self._enqueue(txid, TxOperation(op="SET", table=table, key=key, value=value))

    def update(self, txid: str, table: str, key: Any, value: Any) -> None:
        self._enqueue(txid, TxOperation(op="SET", table=table, key=key, value=value))

    def delete(self, txid: str, table: str, key: Any) -> None:
        self._enqueue(txid, TxOperation(op="DELETE", table=table, key=key))

    def rollback(self, txid: str) -> None:
        """Rollback by dropping pending writes (deferred updates)."""
        with self._lock:
            try:
                self._require_tx(txid)
                self._append_wal({"type": "ROLLBACK", "txid": txid})
                del self._active[txid]
            finally:
                self._release_tx_gate(txid)

    def commit(
        self,
        txid: str,
        consistency_checks: list[Callable[[DatabaseManager], None]] | None = None,
        fail_after_wal: bool = False,
        fail_after_apply_ops: int | None = None,
    ) -> None:
        """Commit a transaction atomically and durably.

        Args:
            txid: Transaction id.
            consistency_checks: Optional validation callbacks to enforce constraints.
            fail_after_wal: Simulates crash after COMMIT is logged but before apply.
            fail_after_apply_ops: Simulates crash after N applied operations.
        """
        with self._lock:
            ctx: TransactionContext | None = None
            try:
                ctx = self._require_tx(txid)

                for op in ctx.operations:
                    self._append_wal(
                        {
                            "type": op.op,
                            "txid": txid,
                            "table": op.table,
                            "key": op.key,
                            "value": op.value,
                        }
                    )

                # Validate constraints on the staged post-transaction state before COMMIT is logged.
                if consistency_checks:
                    staged_db = self._build_staged_db_manager(ctx.operations)
                    for check in consistency_checks:
                        check(staged_db)

                self._append_wal({"type": "COMMIT", "txid": txid})

                if fail_after_wal:
                    raise RuntimeError("Simulated crash after COMMIT WAL record")

                self._apply_operations(ctx.operations, fail_after_apply_ops=fail_after_apply_ops)

                self._save_snapshot()
                # Checkpoint complete: committed state is persisted in snapshot, so WAL can be reset.
                self.wal_path.write_text("", encoding="utf-8")
                del self._active[txid]
            except RuntimeError as exc:
                # Preserve crash-simulation semantics: COMMIT may already be logged and should be recoverable.
                if str(exc) in {
                    "Simulated crash after COMMIT WAL record",
                    "Simulated crash during data application",
                }:
                    raise

                if txid in self._active:
                    self._append_wal({"type": "ROLLBACK", "txid": txid})
                    del self._active[txid]
                raise
            except Exception:
                if txid in self._active:
                    self._append_wal({"type": "ROLLBACK", "txid": txid})
                    del self._active[txid]
                raise
            finally:
                self._release_tx_gate(txid)

    def recover(self) -> None:
        """Recover database by redoing committed transactions from WAL."""
        with self._lock:
            if not self.wal_path.exists():
                return

            records = self._read_wal_records()
            if not records:
                return

            tx_ops: dict[str, list[TxOperation]] = {}
            committed: set[str] = set()
            rolled_back: set[str] = set()

            for rec in records:
                rec_type = rec.get("type")
                txid = rec.get("txid")
                if not txid:
                    continue

                if rec_type in {"SET", "DELETE"}:
                    tx_ops.setdefault(txid, []).append(
                        TxOperation(
                            op=rec_type,
                            table=rec["table"],
                            key=rec["key"],
                            value=rec.get("value"),
                        )
                    )
                elif rec_type == "COMMIT":
                    committed.add(txid)
                elif rec_type == "ROLLBACK":
                    rolled_back.add(txid)

            for txid in committed:
                if txid in rolled_back:
                    continue
                self._apply_operations(tx_ops.get(txid, []), fail_after_apply_ops=None)

            self._save_snapshot()
            self.wal_path.write_text("", encoding="utf-8")

    def select(self, table: str, key: Any) -> Any:
        """Read committed state from the underlying table."""
        tbl = self.db_manager.get_table(self.db_name, table)
        return tbl.select(key)

    def _enqueue(self, txid: str, op: TxOperation) -> None:
        with self._lock:
            ctx = self._require_tx(txid)
            ctx.operations.append(op)

    def _require_tx(self, txid: str) -> TransactionContext:
        if txid not in self._active:
            raise KeyError(f"Transaction '{txid}' is not active")
        return self._active[txid]

    def _release_tx_gate(self, txid: str) -> None:
        if txid in self._gated_txs:
            self._gated_txs.remove(txid)
            self._tx_gate.release()

    def _build_staged_db_manager(self, operations: list[TxOperation]) -> DatabaseManager:
        staged = DatabaseManager()

        for table_name in self.db_manager.list_tables(self.db_name):
            source_table = self.db_manager.get_table(self.db_name, table_name)
            target_table = staged.create_table(
                self.db_name,
                table_name,
                source_table.schema,
                order=source_table.index.order,
                search_key=source_table.search_key,
            )
            for key, value in source_table.get_all():
                target_table.insert(key, value)

        for op in operations:
            table = staged.get_table(self.db_name, op.table)
            if op.op == "SET":
                table.insert(op.key, op.value)
            elif op.op == "DELETE":
                table.delete(op.key)
            else:
                raise ValueError(f"Unknown operation '{op.op}'")

        return staged

    def _apply_operations(
        self,
        operations: list[TxOperation],
        fail_after_apply_ops: int | None,
    ) -> None:
        applied = 0
        for op in operations:
            table = self.db_manager.get_table(self.db_name, op.table)
            if op.op == "SET":
                table.insert(op.key, op.value)
            elif op.op == "DELETE":
                table.delete(op.key)
            else:
                raise ValueError(f"Unknown operation '{op.op}'")

            applied += 1
            if fail_after_apply_ops is not None and applied >= fail_after_apply_ops:
                raise RuntimeError("Simulated crash during data application")

    def _append_wal(self, payload: dict[str, Any]) -> None:
        self._lsn += 1
        rec = {"lsn": self._lsn, **payload}
        line = json.dumps(rec, separators=(",", ":"), default=str)
        with self.wal_path.open("a", encoding="utf-8") as f:
            f.write(line + os.linesep)
            f.flush()
            os.fsync(f.fileno())

    def _read_wal_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        with self.wal_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    # Ignore trailing partial/corrupted record from abrupt crash.
                    continue

        if records:
            self._lsn = max(self._lsn, max(int(r.get("lsn", 0)) for r in records))
        return records

    def _save_snapshot(self) -> None:
        snapshot: dict[str, Any] = {
            "db_name": self.db_name,
            "tables": {},
        }

        for table_name in self.db_manager.list_tables(self.db_name):
            table = self.db_manager.get_table(self.db_name, table_name)
            snapshot["tables"][table_name] = {
                "order": table.index.order,
                "schema": table.schema,
                "search_key": table.search_key,
                "records": table.get_all(),
            }

        self.snapshot_path.write_text(
            json.dumps(snapshot, indent=2, default=str),
            encoding="utf-8",
        )

    def _load_snapshot(self) -> None:
        if not self.snapshot_path.exists():
            return

        data = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
        self.db_name = data.get("db_name", self.db_name)

        if self.db_name not in self.db_manager.databases:
            self.db_manager.create_database(self.db_name)

        existing = set(self.db_manager.list_tables(self.db_name))
        for table_name in existing:
            self.db_manager.drop_table(self.db_name, table_name)

        for table_name, tbl_data in data.get("tables", {}).items():
            table = self.db_manager.create_table(
                self.db_name,
                table_name,
                tbl_data.get("schema"),
                order=tbl_data.get("order", 4),
                search_key=tbl_data.get("search_key"),
            )
            for key, value in tbl_data.get("records", []):
                table.insert(key, value)


def ecommerce_consistency_check(db: DatabaseManager, db_name: str = "__default__") -> None:
    """Consistency checks for Users, Products, and Orders relations.

    Enforces:
    - Users.balance >= 0
    - Products.stock >= 0
    - Every order references existing user and product
    """

    users = db.get_table(db_name, "users")
    products = db.get_table(db_name, "products")
    orders = db.get_table(db_name, "orders")

    user_rows = {k: v for k, v in users.get_all()}
    product_rows = {k: v for k, v in products.get_all()}

    for user_id, row in user_rows.items():
        balance = row.get("balance", 0) if isinstance(row, dict) else 0
        if balance < 0:
            raise ValueError(f"Consistency violation: users[{user_id}].balance < 0")

    for product_id, row in product_rows.items():
        stock = row.get("stock", 0) if isinstance(row, dict) else 0
        if stock < 0:
            raise ValueError(f"Consistency violation: products[{product_id}].stock < 0")

    for order_id, row in orders.get_all():
        if not isinstance(row, dict):
            raise ValueError(f"Consistency violation: orders[{order_id}] must be a dict")

        user_id = row.get("user_id")
        product_id = row.get("product_id")
        if user_id not in user_rows:
            raise ValueError(f"Consistency violation: orders[{order_id}] references missing user {user_id}")
        if product_id not in product_rows:
            raise ValueError(f"Consistency violation: orders[{order_id}] references missing product {product_id}")
