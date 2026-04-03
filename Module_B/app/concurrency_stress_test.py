from __future__ import annotations

import random
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Allow importing Module_A/database package from Module_B/app.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Module_A.database import ACIDTransactionManager, DatabaseManager, ecommerce_consistency_check


def setup_system(data_dir: str, initial_stock: int = 800) -> ACIDTransactionManager:
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

    seed_tx = tm.begin()
    for i in range(1, 201):
        tm.insert(
            seed_tx,
            "users",
            i,
            {"user_id": i, "name": f"User{i}", "balance": 2000, "city": "Gandhinagar"},
        )
    tm.insert(seed_tx, "products", 1, {"product_id": 1, "name": "Seat", "stock": initial_stock, "price": 10})
    # Dedicated product for crash-recovery probe so stress contention cannot consume it.
    tm.insert(seed_tx, "products", 2, {"product_id": 2, "name": "RecoveryProbe", "stock": 1, "price": 1})
    tm.commit(seed_tx, consistency_checks=[lambda dbm: ecommerce_consistency_check(dbm)])
    return tm


def booking_operation(
    tm: ACIDTransactionManager,
    order_id: int,
    user_id: int,
    qty: int = 1,
    simulate_failure: bool = False,
) -> bool:
    tx = tm.begin()
    try:
        user = tm.select("users", user_id)
        product = tm.select("products", 1)
        if user is None or product is None:
            tm.rollback(tx)
            return False

        amount = qty * int(product["price"])
        if int(user["balance"]) < amount or int(product["stock"]) < qty:
            tm.rollback(tx)
            return False

        user_new = dict(user)
        user_new["balance"] -= amount

        product_new = dict(product)
        product_new["stock"] -= qty

        tm.update(tx, "users", user_id, user_new)
        tm.update(tx, "products", 1, product_new)
        tm.insert(tx, "orders", order_id, {"order_id": order_id, "user_id": user_id, "product_id": 1, "amount": amount})

        # Simulate application-level failure before commit to validate rollback path.
        if simulate_failure:
            tm.rollback(tx)
            return False

        tm.commit(tx, consistency_checks=[lambda dbm: ecommerce_consistency_check(dbm)])
        return True
    except Exception:
        try:
            tm.rollback(tx)
        except Exception:
            pass
        return False


def run_stress_test(total_requests: int = 2000, workers: int = 40, initial_stock: int = 800, failure_rate: float = 0.05) -> None:
    data_dir = Path(__file__).resolve().parent / "stress_data"
    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    tm = setup_system(str(data_dir), initial_stock=initial_stock)

    start = time.perf_counter()
    success = 0
    failures = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for i in range(total_requests):
            user_id = random.randint(1, 200)
            inject_failure = random.random() < failure_rate
            futures.append(executor.submit(booking_operation, tm, 100000 + i, user_id, 1, inject_failure))

        for future in as_completed(futures):
            if future.result():
                success += 1
            else:
                failures += 1

    elapsed = time.perf_counter() - start

    # Crash simulation: write COMMIT in WAL but raise before apply; recovery must fix state.
    crash_tx = tm.begin()
    user = tm.select("users", 1)
    product = tm.select("products", 2)
    if user and product and int(product["stock"]) >= 1 and int(user["balance"]) >= int(product["price"]):
        amount = int(product["price"])
        user_new = dict(user)
        product_new = dict(product)
        user_new["balance"] -= amount
        product_new["stock"] -= 1
        tm.update(crash_tx, "users", 1, user_new)
        tm.update(crash_tx, "products", 2, product_new)
        tm.insert(crash_tx, "orders", 999999, {"order_id": 999999, "user_id": 1, "product_id": 2, "amount": amount})
        try:
            tm.commit(
                crash_tx,
                consistency_checks=[lambda dbm: ecommerce_consistency_check(dbm)],
                fail_after_wal=True,
            )
        except RuntimeError:
            pass

    # Restart + recovery validation.
    db2 = DatabaseManager()
    db2.create_table("users", schema={"user_id": int, "name": str, "balance": int, "city": str}, order=8, search_key="user_id")
    db2.create_table("products", schema={"product_id": int, "name": str, "stock": int, "price": int}, order=8, search_key="product_id")
    db2.create_table("orders", schema={"order_id": int, "user_id": int, "product_id": int, "amount": int}, order=8, search_key="order_id")
    tm2 = ACIDTransactionManager(db2, storage_dir=str(data_dir))

    ecommerce_consistency_check(tm2.db_manager)
    recovered_order = tm2.select("orders", 999999)

    product_after = tm2.select("products", 1)
    assert product_after is not None
    remaining_stock = int(product_after["stock"])
    sold = initial_stock - remaining_stock
    order_count = len(tm2.db_manager.get_table("orders").get_all())

    # Main contention workload operates on product 1 only, so sold units must match successful bookings.
    expected_total_orders = success + (1 if recovered_order is not None else 0)
    assert sold == success, "Stock delta does not match successful bookings"
    assert order_count == expected_total_orders, "Order count mismatch with successful commits"
    assert sold <= initial_stock, "Oversell detected"

    throughput = total_requests / elapsed if elapsed > 0 else 0.0
    print("==== Assignment 3 Stress Test Report ====")
    print(f"Total requests: {total_requests}")
    print(f"Workers: {workers}")
    print(f"Initial stock: {initial_stock}")
    print(f"Failure injection rate: {failure_rate:.2f}")
    print(f"Successful bookings: {success}")
    print(f"Failed bookings: {failures}")
    print(f"Orders committed (including recovery tx): {order_count}")
    print(f"Stock remaining: {remaining_stock}")
    print(f"Elapsed time: {elapsed:.3f} s")
    print(f"Throughput: {throughput:.2f} req/s")
    print(f"Recovery check (order 999999 present): {recovered_order is not None}")
    print("Consistency check: PASS")


if __name__ == "__main__":
    run_stress_test(total_requests=1500, workers=30, initial_stock=800, failure_rate=0.05)
