from __future__ import annotations

import shutil
import threading
from pathlib import Path

from database import ACIDTransactionManager, DatabaseManager, ecommerce_consistency_check


def setup_manager(data_dir: str) -> ACIDTransactionManager:
    db = DatabaseManager()
    db.create_table(
        "users",
        schema={"user_id": int, "name": str, "balance": int, "city": str},
        order=8,
        search_key="user_id",
    )
    db.create_table(
        "products",
        schema={"product_id": int, "name": str, "stock": int, "price": int},
        order=8,
        search_key="product_id",
    )
    db.create_table(
        "orders",
        schema={"order_id": int, "user_id": int, "product_id": int, "amount": int},
        order=8,
        search_key="order_id",
    )

    tm = ACIDTransactionManager(db, storage_dir=data_dir)

    tx = tm.begin()
    tm.insert(tx, "users", 1, {"user_id": 1, "name": "Asha", "balance": 1000, "city": "Ahmedabad"})
    tm.insert(tx, "users", 2, {"user_id": 2, "name": "Ravi", "balance": 1200, "city": "Surat"})
    tm.insert(tx, "products", 1, {"product_id": 1, "name": "Notebook", "stock": 20, "price": 50})
    tm.insert(tx, "products", 2, {"product_id": 2, "name": "Pen", "stock": 100, "price": 10})
    tm.commit(tx, consistency_checks=[lambda dbm: ecommerce_consistency_check(dbm)])
    return tm


def place_order(
    tm: ACIDTransactionManager,
    order_id: int,
    user_id: int,
    product_id: int,
    qty: int,
    simulate_fail: bool = False,
) -> None:
    tx = tm.begin()

    user = tm.select("users", user_id)
    product = tm.select("products", product_id)

    if user is None or product is None:
        tm.rollback(tx)
        raise ValueError("Missing user or product")

    total_cost = qty * int(product["price"])
    if int(user["balance"]) < total_cost or int(product["stock"]) < qty:
        tm.rollback(tx)
        raise ValueError("Insufficient balance or stock")

    user_new = dict(user)
    user_new["balance"] = int(user_new["balance"]) - total_cost

    product_new = dict(product)
    product_new["stock"] = int(product_new["stock"]) - qty

    tm.update(tx, "users", user_id, user_new)
    tm.update(tx, "products", product_id, product_new)
    tm.insert(
        tx,
        "orders",
        order_id,
        {
            "order_id": order_id,
            "user_id": user_id,
            "product_id": product_id,
            "amount": total_cost,
        },
    )

    tm.commit(
        tx,
        consistency_checks=[lambda dbm: ecommerce_consistency_check(dbm)],
        fail_after_wal=simulate_fail,
    )


def test_atomicity_and_recovery(base_dir: str) -> None:
    print("[Atomicity] Simulating failure after COMMIT WAL and before apply...")
    tm = setup_manager(base_dir)

    try:
        place_order(tm, order_id=1001, user_id=1, product_id=1, qty=2, simulate_fail=True)
    except RuntimeError:
        pass

    # Restart simulation: reconstruct manager and recover from WAL.
    db2 = DatabaseManager()
    db2.create_table("users", schema={"user_id": int, "name": str, "balance": int, "city": str}, order=8, search_key="user_id")
    db2.create_table("products", schema={"product_id": int, "name": str, "stock": int, "price": int}, order=8, search_key="product_id")
    db2.create_table("orders", schema={"order_id": int, "user_id": int, "product_id": int, "amount": int}, order=8, search_key="order_id")
    tm2 = ACIDTransactionManager(db2, storage_dir=base_dir)

    user = tm2.select("users", 1)
    product = tm2.select("products", 1)
    order = tm2.select("orders", 1001)

    assert user is not None and user["balance"] == 900
    assert product is not None and product["stock"] == 18
    assert order is not None and order["amount"] == 100
    print("[Atomicity + Durability + Recovery] PASS")


def test_isolation_with_concurrency(base_dir: str) -> None:
    print("[Isolation] Running concurrent order placements...")
    tm = setup_manager(base_dir)

    failures = 0
    lock = threading.Lock()

    def worker(i: int) -> None:
        nonlocal failures
        try:
            # Each tries to buy 1 notebook (price=50, initial stock=20).
            place_order(tm, order_id=2000 + i, user_id=2, product_id=1, qty=1)
        except Exception:
            with lock:
                failures += 1

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    product = tm.select("products", 1)
    assert product is not None

    sold = 20 - int(product["stock"])
    assert sold == 20
    assert failures >= 10
    print("[Isolation under contention] PASS")


def main() -> None:
    base = Path(__file__).resolve().parent / "acid_demo_data"
    if base.exists():
        shutil.rmtree(base)
    base.mkdir(parents=True, exist_ok=True)

    test_atomicity_and_recovery(str(base))

    if base.exists():
        shutil.rmtree(base)
    base.mkdir(parents=True, exist_ok=True)

    test_isolation_with_concurrency(str(base))
    print("All ACID validation checks passed.")


if __name__ == "__main__":
    main()
