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
        # Keep a reference to the database manager because every transaction
        # ultimately reads from and writes back into those B+ Tree-backed tables.
        # The manager does not duplicate row data; it only controls when tables change.
        self.db_manager = db_manager
        self.db_name = db_name

        # A re-entrant lock protects internal transaction bookkeeping so that
        # commit, rollback, and recovery logic cannot race each other.
            # One lock protects bookkeeping; one gate serializes whole transactions.
        self._lock = threading.RLock()

        # A simple gate lock enforces serialized execution across whole transactions.
        # This is intentionally conservative: it trades throughput for correctness.
            # Track the gate owner so release happens exactly once.
        self._tx_gate = threading.Lock()

        # Track which transactions still own the gate so we release it exactly once.
        self._gated_txs: set[str] = set()

        # Active transactions hold deferred operations until COMMIT or ROLLBACK.
            # Deferred operations live here until commit or rollback.
        self._active: dict[str, TransactionContext] = {}

        # Logical sequence number used to order WAL records and help recovery.
            # LSNs make WAL replay and debugging easier.
        self._lsn = 0

        # Storage files live under the selected directory so crash recovery can
        # reconstruct the last durable state after a restart.
            # Store recovery files on disk so restart can reconstruct committed work.
        self.storage_path = Path(storage_dir)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.wal_path = self.storage_path / "wal.log"
        self.snapshot_path = self.storage_path / "snapshot.json"

        # Restore the last snapshot first, then replay any committed WAL records.
            # Load the checkpoint first, then replay any surviving log tail.
        self._load_snapshot()
        self.recover()

    def begin(self) -> str:
        """Begin a new transaction and return transaction id."""
        # Gate the entire transaction so concurrent transactions cannot interleave.
        # Serialize full transaction lifetime (BEGIN -> COMMIT/ROLLBACK).
        self._tx_gate.acquire()
        with self._lock:
            try:
                # Generate a unique transaction id and register an empty deferred log.
                txid = uuid.uuid4().hex
                self._active[txid] = TransactionContext(txid=txid)
                self._gated_txs.add(txid)

                # Record BEGIN in the WAL so recovery can tell that this transaction existed.
                self._append_wal({"type": "BEGIN", "txid": txid})
                return txid
            except Exception:
                # If setup fails, immediately release the gate to avoid deadlocking future work.
                self._tx_gate.release()
                raise

    def insert(self, txid: str, table: str, key: Any, value: Any) -> None:
        # Inserts are staged, not applied immediately, so a later rollback stays cheap.
        self._enqueue(txid, TxOperation(op="SET", table=table, key=key, value=value))

    def update(self, txid: str, table: str, key: Any, value: Any) -> None:
        # Updates reuse the same deferred write path as inserts because both replace
        # the full record stored inside the B+ Tree leaf.
        self._enqueue(txid, TxOperation(op="SET", table=table, key=key, value=value))

    def delete(self, txid: str, table: str, key: Any) -> None:
        # Deletes are also deferred so the transaction can still be aborted cleanly.
        self._enqueue(txid, TxOperation(op="DELETE", table=table, key=key))

    def rollback(self, txid: str) -> None:
        """Rollback by dropping pending writes (deferred updates)."""
        with self._lock:
            try:
                # Confirm the transaction is still active before logging rollback.
                self._require_tx(txid)

                # WAL marks the explicit abort so recovery can ignore this tx.
                self._append_wal({"type": "ROLLBACK", "txid": txid})

                # Remove all staged operations in one shot; no table state changes happen.
                del self._active[txid]
            finally:
                # Always release the gate, even if a bookkeeping step fails.
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
                # Read the staged transaction state only after the lock is acquired.
                ctx = self._require_tx(txid)

                # Log every intended mutation before applying anything to the B+ Trees.
                # This is the core write-ahead logging guarantee.
                for op in ctx.operations:
                    # WAL first: every intended change is recorded before touching the B+ Trees.
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
                # This checks business rules against the would-be final database image.
                if consistency_checks:
                    staged_db = self._build_staged_db_manager(ctx.operations)
                    for check in consistency_checks:
                        check(staged_db)

                # Only after the staged state is valid do we declare the tx committed in WAL.
                self._append_wal({"type": "COMMIT", "txid": txid})

                if fail_after_wal:
                    # Used for crash-recovery demonstrations: simulate failure after COMMIT is durable.
                    raise RuntimeError("Simulated crash after COMMIT WAL record")

                # Apply the deferred operations to the live database, which is the real B+ Tree state.
                self._apply_operations(ctx.operations, fail_after_apply_ops=fail_after_apply_ops)

                # Persist the new committed state to a snapshot so future restarts are faster.
                self._save_snapshot()
                # Checkpoint complete: committed state is persisted in snapshot, so WAL can be reset.
                self.wal_path.write_text("", encoding="utf-8")

                # Remove the active context only after the durable state is safely written.
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
            # If there is no WAL file yet, there is nothing to replay.
            if not self.wal_path.exists():
                return

            # Parse the log into per-transaction buckets so we can tell what committed.
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
                    # Rebuild the deferred operation list exactly as it was logged.
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
                # Redo only transactions that reached COMMIT.
                self._apply_operations(tx_ops.get(txid, []), fail_after_apply_ops=None)

            # Recovery leaves the database in a clean snapshot state with no stale WAL.
            self._save_snapshot()
            self.wal_path.write_text("", encoding="utf-8")

    def select(self, table: str, key: Any) -> Any:
        """Read committed state from the underlying table."""
        tbl = self.db_manager.get_table(self.db_name, table)
        return tbl.select(key)

    def _enqueue(self, txid: str, op: TxOperation) -> None:
        with self._lock:
            # Staging inside the transaction context keeps the live B+ Trees unchanged until commit.
            ctx = self._require_tx(txid)
            ctx.operations.append(op)

    def _require_tx(self, txid: str) -> TransactionContext:
        if txid not in self._active:
            raise KeyError(f"Transaction '{txid}' is not active")
        return self._active[txid]

    def _release_tx_gate(self, txid: str) -> None:
        if txid in self._gated_txs:
            # Release only once per transaction; this avoids double-release bugs.
            self._gated_txs.remove(txid)
            self._tx_gate.release()

    def _build_staged_db_manager(self, operations: list[TxOperation]) -> DatabaseManager:
        # Clone the current DB state so consistency checks run on the post-transaction image.
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
                staged.insert_record(self.db_name, op.table, op.key, op.value)
            elif op.op == "DELETE":
                staged.delete_record(self.db_name, op.table, op.key)
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
            if op.op == "SET":
                # SET means insert/update the complete record for that key.
                self.db_manager.insert_record(self.db_name, op.table, op.key, op.value)
            elif op.op == "DELETE":
                # DELETE removes the key from the target table's B+ Tree.
                self.db_manager.delete_record(self.db_name, op.table, op.key)
            else:
                raise ValueError(f"Unknown operation '{op.op}'")

            applied += 1
            if fail_after_apply_ops is not None and applied >= fail_after_apply_ops:
                # Another crash-injection hook used to verify partial application never survives.
                raise RuntimeError("Simulated crash during data application")

    def _append_wal(self, payload: dict[str, Any]) -> None:
        # The WAL is append-only and fsynced so that a power loss cannot erase committed intent.
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
                    # Skip malformed trailing data instead of failing recovery entirely.
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    # Ignore trailing partial/corrupted record from abrupt crash.
                    continue

        if records:
            self._lsn = max(self._lsn, max(int(r.get("lsn", 0)) for r in records))
        return records

    def _save_snapshot(self) -> None:
        # The snapshot is a compact durable image of all tables and FK metadata.
        snapshot: dict[str, Any] = {
            "db_name": self.db_name,
            "tables": {},
            "foreign_keys": self.db_manager.list_foreign_keys(self.db_name),
        }

        for table_name in self.db_manager.list_tables(self.db_name):
            table = self.db_manager.get_table(self.db_name, table_name)
            snapshot["tables"][table_name] = {
                "order": table.index.order,
                "schema": table.schema,
                "search_key": table.search_key,
                # Store every record so restart can rebuild the exact B+ Tree state.
                "records": table.get_all(),
            }

        self.snapshot_path.write_text(
            json.dumps(snapshot, indent=2, default=str),
            encoding="utf-8",
        )

    def _load_snapshot(self) -> None:
        # Snapshot load is optional; the system can still start from an empty database.
        if not self.snapshot_path.exists():
            return

        data = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
        self.db_name = data.get("db_name", self.db_name)

        if self.db_name not in self.db_manager.databases:
            # Recreate the namespace before restoring tables into it.
            self.db_manager.create_database(self.db_name)

        existing = set(self.db_manager.list_tables(self.db_name))
        for table_name in existing:
            # Clear any pre-existing tables so the snapshot becomes the source of truth.
            self.db_manager.drop_table(self.db_name, table_name)

        for table_name, tbl_data in data.get("tables", {}).items():
            # Rebuild each table and repopulate it from the serialized records.
            table = self.db_manager.create_table(
                self.db_name,
                table_name,
                tbl_data.get("schema"),
                order=tbl_data.get("order", 4),
                search_key=tbl_data.get("search_key"),
            )
            for key, value in tbl_data.get("records", []):
                table.insert(key, value)

        for table_name, constraints in data.get("foreign_keys", {}).items():
            for constraint in constraints:
                # Restore FK metadata after table data exists so referential checks are valid.
                self.db_manager.add_foreign_key(
                    table_name,
                    constraint["column"],
                    constraint["referenced_table"],
                    referenced_column=constraint.get("referenced_column"),
                    db_name=self.db_name,
                    referenced_db_name=constraint.get("referenced_db"),
                    on_delete=constraint.get("on_delete", "restrict"),
                )


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


def travel_consistency_check(db: DatabaseManager, db_name: str = "__default__") -> None:
    """Consistency checks for Member, Trip, and Booking relations.

    Enforces:
    - Trip.Capacity >= 0 and 0 <= Trip.BookedSeats <= Trip.Capacity
    - Booking.MemberID and Booking.TripID reference existing rows
    - Seat numbers are unique per trip for confirmed bookings
    """

    members = db.get_table(db_name, "member")
    trips = db.get_table(db_name, "trip")
    bookings = db.get_table(db_name, "booking")

    member_rows = {k: v for k, v in members.get_all()}
    trip_rows = {k: v for k, v in trips.get_all()}

    confirmed_by_trip: dict[int, set[str]] = {}
    confirmed_counts: dict[int, int] = {}

    for trip_id, row in trip_rows.items():
        if not isinstance(row, dict):
            raise ValueError(f"Consistency violation: trip[{trip_id}] must be a dict")

        capacity = int(row.get("Capacity", 0))
        booked = int(row.get("BookedSeats", 0))
        if capacity < 0:
            raise ValueError(f"Consistency violation: trip[{trip_id}].Capacity < 0")
        if booked < 0 or booked > capacity:
            raise ValueError(
                f"Consistency violation: trip[{trip_id}].BookedSeats={booked} exceeds capacity={capacity}"
            )

    for booking_id, row in bookings.get_all():
        if not isinstance(row, dict):
            raise ValueError(f"Consistency violation: booking[{booking_id}] must be a dict")

        member_id = row.get("MemberID")
        trip_id = row.get("TripID")
        seat_no = row.get("SeatNo")
        status = row.get("Status")

        if member_id not in member_rows:
            raise ValueError(
                f"Consistency violation: booking[{booking_id}] references missing member {member_id}"
            )
        if trip_id not in trip_rows:
            raise ValueError(
                f"Consistency violation: booking[{booking_id}] references missing trip {trip_id}"
            )

        if status == "confirmed":
            if seat_no is None:
                raise ValueError(f"Consistency violation: booking[{booking_id}] confirmed without SeatNo")

            seat_text = str(seat_no)
            seen = confirmed_by_trip.setdefault(int(trip_id), set())
            if seat_text in seen:
                raise ValueError(
                    f"Consistency violation: duplicate confirmed seat '{seat_text}' on trip {trip_id}"
                )
            seen.add(seat_text)
            confirmed_counts[int(trip_id)] = confirmed_counts.get(int(trip_id), 0) + 1

    for trip_id, row in trip_rows.items():
        booked = int(row.get("BookedSeats", 0)) if isinstance(row, dict) else 0
        if confirmed_counts.get(int(trip_id), 0) != booked:
            raise ValueError(
                f"Consistency violation: trip[{trip_id}].BookedSeats={booked} does not match "
                f"confirmed bookings={confirmed_counts.get(int(trip_id), 0)}"
            )
