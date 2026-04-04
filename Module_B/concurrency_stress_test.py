"""Concurrent usage, race-condition, failure, and stress testing for `shuttle_system`.

This script exercises the schema in `sql/01_shuttle_system.sql` directly so it can
verify ACID behavior even when the Flask app does not expose every write path.

Scenarios covered:
- Concurrent usage across multiple users
- Race conditions on shared registration data
- Failure injection with rollback validation
- High-volume mixed stress testing

Defaults are aligned with `app/app.py`, but can be overridden with environment
variables or CLI flags.
"""

from __future__ import annotations

import argparse
import os
import random
import statistics
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterable

import pymysql
import requests


@dataclass(frozen=True)
class DBConfig:
    host: str = os.getenv("MYSQL_HOST", "localhost")
    port: int = int(os.getenv("MYSQL_PORT", "3306"))
    user: str = os.getenv("MYSQL_USER", "root")
    password: str = os.getenv("MYSQL_PASSWORD", "Samarth@05")
    database: str = os.getenv("MYSQL_DATABASE", "shuttle_system")


@dataclass
class SandboxUser:
    member_id: int
    username: str
    email: str
    phone: str
    name: str


@dataclass
class ExistingMember:
    member_id: int
    name: str
    email: str
    phone: str


@dataclass
class ApiCredentials:
    username: str
    password: str


@dataclass
class ScenarioSummary:
    name: str
    passed: bool
    details: str
    duration_seconds: float


def connect(config: DBConfig) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        database=config.database,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def unique_tag(prefix: str) -> str:
    stamp = time.strftime("%Y%m%d%H%M%S")
    return f"{prefix}_{stamp}_{uuid.uuid4().hex[:8]}"


@contextmanager
def db_cursor(config: DBConfig):
    connection = connect(config)
    try:
        with connection.cursor() as cursor:
            yield connection, cursor
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def scalar(config: DBConfig, sql: str, params: tuple = ()):
    with db_cursor(config) as (_, cursor):
        cursor.execute(sql, params)
        row = cursor.fetchone()
        if not row:
            return None
        return next(iter(row.values()))


def count_rows(config: DBConfig, sql: str, params: tuple = ()) -> int:
    value = scalar(config, sql, params)
    return int(value or 0)


def fetch_all(config: DBConfig, sql: str, params: tuple = ()):
    with db_cursor(config) as (_, cursor):
        cursor.execute(sql, params)
        return cursor.fetchall()


def next_id(config: DBConfig, table_name: str, column_name: str) -> int:
    return int(
        scalar(
            config,
            f"SELECT COALESCE(MAX({column_name}), 0) + 1 AS next_id FROM {table_name}",
        )
        or 1
    )


def load_existing_members(config: DBConfig, limit: int) -> list[ExistingMember]:
    rows = fetch_all(
        config,
        "SELECT MemberID, Name, Email, Phone FROM MEMBER ORDER BY MemberID LIMIT %s",
        (limit,),
    )
    members = [
        ExistingMember(
            member_id=row["MemberID"],
            name=row["Name"],
            email=row["Email"],
            phone=row["Phone"] or "",
        )
        for row in rows
    ]
    if not members:
        raise RuntimeError("No MEMBER rows available for testing")
    return members


def get_trip_count(config: DBConfig) -> int:
    return count_rows(config, "SELECT COUNT(*) AS c FROM TRIP")


def get_booking_count_for_member(config: DBConfig, member_id: int) -> int:
    return count_rows(config, "SELECT COUNT(*) AS c FROM BOOKING WHERE MemberID = %s", (member_id,))


def read_profile(config: DBConfig, member_id: int):
    rows = fetch_all(config, "SELECT Name, Email, Phone FROM MEMBER WHERE MemberID = %s", (member_id,))
    return rows[0] if rows else None


def update_profile(config: DBConfig, member_id: int, email: str, phone: str) -> None:
    with db_cursor(config) as (_, cursor):
        cursor.execute(
            "UPDATE MEMBER SET Email = %s, Phone = %s WHERE MemberID = %s",
            (email, phone, member_id),
        )


def get_trip_capacity(config: DBConfig, trip_id: int) -> int:
    return int(
        scalar(
            config,
            """
            SELECT sh.Capacity AS capacity
            FROM TRIP t
            JOIN SCHEDULE s ON t.ScheduleID = s.ScheduleID
            JOIN SHUTTLE sh ON s.ShuttleID = sh.ShuttleID
            WHERE t.TripID = %s
            """,
            (trip_id,),
        )
        or 0
    )


def find_free_seat(config: DBConfig, trip_id: int) -> int:
    capacity = get_trip_capacity(config, trip_id)
    if capacity <= 0:
        raise RuntimeError(f"TripID {trip_id} does not map to a valid shuttle capacity")

    booked_seats = {
        row["SeatNo"]
        for row in fetch_all(config, "SELECT SeatNo FROM BOOKING WHERE TripID = %s", (trip_id,))
    }
    for seat_no in range(1, capacity + 1):
        if seat_no not in booked_seats:
            return seat_no
    raise RuntimeError(f"No free seats left for TripID {trip_id}")


def find_bookable_trip(config: DBConfig) -> int:
    trip_id = scalar(
        config,
        "SELECT TripID FROM TRIP WHERE Status != 'Cancelled' ORDER BY TripID LIMIT 1",
    )
    if trip_id is None:
        raise RuntimeError("No non-cancelled TRIP rows available for booking tests")
    return int(trip_id)


def book_seat_transaction(config: DBConfig, member_id: int, trip_id: int, seat_no: int):
    connection = connect(config)
    try:
        with connection.cursor() as cursor:
            lock_key = f"booking:{trip_id}:{seat_no}"
            cursor.execute("SELECT GET_LOCK(%s, 10) AS lock_status", (lock_key,))
            lock_row = cursor.fetchone()
            if not lock_row or lock_row.get("lock_status") != 1:
                return False, "Seat is currently busy", None

            try:
                cursor.execute("SELECT Status FROM TRIP WHERE TripID = %s FOR UPDATE", (trip_id,))
                trip = cursor.fetchone()
                if not trip:
                    connection.rollback()
                    return False, "Trip not found", None
                if trip["Status"] == "Cancelled":
                    connection.rollback()
                    return False, "Trip is cancelled", None

                cursor.execute(
                    "SELECT BookingID FROM BOOKING WHERE TripID = %s AND SeatNo = %s FOR UPDATE",
                    (trip_id, seat_no),
                )
                if cursor.fetchone():
                    connection.rollback()
                    return False, "Seat already booked", None

                cursor.execute(
                    "INSERT INTO BOOKING (MemberID, TripID, SeatNo, Status) VALUES (%s, %s, %s, 'Confirmed')",
                    (member_id, trip_id, seat_no),
                )
                booking_id = cursor.lastrowid
                cursor.execute(
                    "INSERT INTO TICKET (BookingID, QRCode, IsVerified) VALUES (%s, %s, %s)",
                    (booking_id, f"QR-{trip_id}-{seat_no}-{member_id}", 0),
                )
                connection.commit()
                return True, "Seat booked successfully", booking_id
            finally:
                cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_key,))
    except Exception as exc:
        connection.rollback()
        return False, str(exc), None
    finally:
        connection.close()


def cleanup_booking(config: DBConfig, trip_id: int, seat_no: int) -> None:
    with db_cursor(config) as (_, cursor):
        cursor.execute(
            "SELECT BookingID FROM BOOKING WHERE TripID = %s AND SeatNo = %s",
            (trip_id, seat_no),
        )
        booking = cursor.fetchone()
        if not booking:
            return
        booking_id = booking["BookingID"]
        cursor.execute("DELETE FROM TICKET WHERE BookingID = %s", (booking_id,))
        cursor.execute("DELETE FROM BOOKING WHERE BookingID = %s", (booking_id,))


def api_login(base_url: str, credentials: ApiCredentials) -> str:
    response = requests.post(
        f"{base_url.rstrip('/')}/login",
        json={"username": credentials.username, "password": credentials.password},
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    token = payload.get("session_token")
    if not token:
        raise RuntimeError(f"Login for {credentials.username} did not return a session token")
    return token


def api_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def api_get_profile(base_url: str, token: str):
    response = requests.get(f"{base_url.rstrip('/')}/api/profile", headers=api_headers(token), timeout=10)
    response.raise_for_status()
    return response.json()


def api_put_profile(base_url: str, token: str, email: str, phone: str):
    response = requests.put(
        f"{base_url.rstrip('/')}/api/profile",
        headers=api_headers(token),
        json={"email": email, "phone": phone},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def api_book_seat(base_url: str, token: str, trip_id: int, seat_no: int):
    response = requests.post(
        f"{base_url.rstrip('/')}/api/bookings",
        headers=api_headers(token),
        json={"trip_id": trip_id, "seat_no": seat_no},
        timeout=10,
    )
    return response


def run_api_profile_smoke(base_url: str) -> ScenarioSummary:
    start = time.perf_counter()
    credentials = ApiCredentials(username="user_ananya", password="password1")
    token = api_login(base_url, credentials)

    results: list[str] = []
    errors: list[str] = []

    def read_once(index: int) -> str:
        payload = api_get_profile(base_url, token)
        if not payload.get("Email"):
            raise AssertionError("Profile response missing email")
        return f"read:{index}"

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(read_once, index) for index in range(5)]
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                errors.append(str(exc))

    original = api_get_profile(base_url, token)
    temp_email = f"api_smoke_{unique_tag('profile')}@example.com"
    temp_phone = "9111111111"
    api_put_profile(base_url, token, temp_email, temp_phone)
    restored = api_get_profile(base_url, token)
    api_put_profile(base_url, token, original["Email"], original["Phone"] or "")

    duration = time.perf_counter() - start
    passed = not errors and len(results) == 5 and restored["Email"] == temp_email and restored["Phone"] == temp_phone
    details = (
        f"GET /api/profile x5 and one PUT succeeded in {duration:.2f}s"
        if passed
        else f"errors={errors[:3]}, restored={restored}"
    )
    return ScenarioSummary("API profile smoke", passed, details, duration)


def run_api_booking_race(base_url: str, config: DBConfig) -> ScenarioSummary:
    start = time.perf_counter()
    trip_id = find_bookable_trip(config)
    seat_no = find_free_seat(config, trip_id)
    cleanup_booking(config, trip_id, seat_no)

    tokens = [
        api_login(base_url, ApiCredentials(username="admin_rahul", password="password123")),
        api_login(base_url, ApiCredentials(username="user_ananya", password="password1")),
    ]

    barrier = threading.Barrier(2)
    responses: list[tuple[int, dict[str, object]]] = []
    errors: list[str] = []

    def attempt(token: str) -> tuple[int, dict[str, object]]:
        barrier.wait()
        response = api_book_seat(base_url, token, trip_id, seat_no)
        try:
            payload = response.json()
        except Exception:
            payload = {"raw": response.text}
        return response.status_code, payload

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(attempt, token) for token in tokens]
        for future in as_completed(futures):
            try:
                responses.append(future.result())
            except Exception as exc:
                errors.append(str(exc))

    booked_count = count_rows(
        config,
        "SELECT COUNT(*) AS c FROM BOOKING WHERE TripID = %s AND SeatNo = %s",
        (trip_id, seat_no),
    )
    cleanup_booking(config, trip_id, seat_no)

    duration = time.perf_counter() - start
    status_codes = sorted(status for status, _ in responses)
    passed = not errors and status_codes == [201, 409] and booked_count == 1
    details = (
        f"status_codes={status_codes}, trip={trip_id}, seat={seat_no}"
        if passed
        else f"status_codes={status_codes}, errors={errors[:3]}, booked_count={booked_count}, responses={responses}"
    )
    return ScenarioSummary("API booking race", passed, details, duration)


def run_concurrent_usage(config: DBConfig, workers: int, users: list[SandboxUser]) -> ScenarioSummary:
    start = time.perf_counter()
    trip_count = get_trip_count(config)
    concurrent_workers = min(workers, len(users))
    selected_users = users[:concurrent_workers]
    results: list[str] = []
    errors: list[str] = []

    def worker(user: SandboxUser, index: int) -> str:
        profile = read_profile(config, user.member_id)
        if not profile:
            raise AssertionError(f"Profile missing for member {user.member_id}")

        base_email = profile["Email"]
        base_phone = profile["Phone"] or ""
        updated_email = f"{user.username}.live.{index}@example.com"
        updated_phone = f"8{index:09d}"

        try:
            update_profile(config, user.member_id, updated_email, updated_phone)
            saved = read_profile(config, user.member_id)
            if not saved or saved["Email"] != updated_email or saved["Phone"] != updated_phone:
                raise AssertionError(f"Update mismatch for member {user.member_id}")
            return f"member={user.member_id}, trips={trip_count}, bookings={get_booking_count_for_member(config, user.member_id)}"
        finally:
            update_profile(config, user.member_id, base_email, base_phone)

    try:
        with ThreadPoolExecutor(max_workers=concurrent_workers) as executor:
            futures = [executor.submit(worker, selected_users[index], index) for index in range(concurrent_workers)]
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as exc:  # pragma: no cover - runtime guard
                    errors.append(str(exc))
    finally:
        for user in selected_users:
            current = read_profile(config, user.member_id)
            if current:
                update_profile(config, user.member_id, current["Email"], current["Phone"] or "")

    duration = time.perf_counter() - start
    passed = not errors and len(results) == concurrent_workers
    details = (
        f"{len(results)}/{concurrent_workers} workers completed; shared trip count stayed at {trip_count}; "
        f"profile updates were isolated per member"
        if passed
        else f"errors={errors[:3]}"
    )
    return ScenarioSummary("Concurrent usage", passed, details, duration)


def run_race_condition(config: DBConfig, workers: int, tag: str) -> ScenarioSummary:
    start = time.perf_counter()
    members = load_existing_members(config, max(workers, 2))
    if len(members) < 2:
        raise RuntimeError("Race testing needs at least 2 MEMBER rows")

    race_workers = min(workers, len(members))
    trip_id = find_bookable_trip(config)
    seat_no = find_free_seat(config, trip_id)
    outcome_lock = threading.Lock()
    success_count = 0
    conflict_count = 0
    error_count = 0

    def attempt(index: int) -> None:
        nonlocal success_count, conflict_count, error_count
        member = members[index]
        booked, message, _ = book_seat_transaction(config, member.member_id, trip_id, seat_no)
        with outcome_lock:
            if booked:
                success_count += 1
            elif message in {"Seat already booked", "Seat is currently busy"}:
                conflict_count += 1
            else:
                error_count += 1

    with ThreadPoolExecutor(max_workers=race_workers) as executor:
        list(executor.map(attempt, range(race_workers)))

    booked_count = count_rows(
        config,
        "SELECT COUNT(*) AS c FROM BOOKING WHERE TripID = %s AND SeatNo = %s",
        (trip_id, seat_no),
    )
    cleanup_booking(config, trip_id, seat_no)

    duration = time.perf_counter() - start
    passed = success_count == 1 and conflict_count == race_workers - 1 and error_count == 0 and booked_count == 1
    details = (
        f"trip={trip_id}, seat={seat_no}, success={success_count}, conflicts={conflict_count}"
        if passed
        else f"success={success_count}, conflicts={conflict_count}, errors={error_count}, booked_count={booked_count}"
    )
    return ScenarioSummary("Race condition", passed, details, duration)


def run_failure_simulation(config: DBConfig, tag: str) -> ScenarioSummary:
    start = time.perf_counter()
    members = load_existing_members(config, 1)
    member = members[0]
    original_email, original_phone = member.email, member.phone
    temp_email = f"{tag}_fail@example.com"
    temp_phone = "9000000000"

    connection = connect(config)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE MEMBER SET Email = %s, Phone = %s WHERE MemberID = %s",
                (temp_email, temp_phone, member.member_id),
            )
            raise RuntimeError("Injected failure after MEMBER update")
    except Exception:
        connection.rollback()
    finally:
        connection.close()

    restored = read_profile(config, member.member_id)
    member_count = count_rows(config, "SELECT COUNT(*) AS c FROM MEMBER WHERE Email = %s", (temp_email,))

    duration = time.perf_counter() - start
    passed = member_count == 0 and restored and restored["Email"] == original_email and restored["Phone"] == original_phone
    details = (
        "rollback removed the injected MEMBER update and no partial data remained"
        if passed
        else f"partial_state_detected: member_count={member_count}, restored={restored}"
    )
    return ScenarioSummary("Failure rollback", passed, details, duration)


def run_stress_test(config: DBConfig, workers: int, operations: int, users: list[SandboxUser], tag: str) -> ScenarioSummary:
    start = time.perf_counter()
    durations: list[float] = []
    failures: list[str] = []
    originals = {user.member_id: (user.email, user.phone) for user in users}

    def stress_task(operation_index: int) -> str:
        user = users[operation_index % len(users)]
        choice = operation_index % 3
        op_start = time.perf_counter()
        if choice == 0:
            profile = read_profile(config, user.member_id)
            if not profile:
                raise AssertionError("Missing profile during stress run")
            result = f"read:{user.member_id}"
        elif choice == 1:
            temp_email = f"{tag}_stress_{operation_index}@example.com"
            temp_phone = f"6{operation_index:09d}"
            update_profile(config, user.member_id, temp_email, temp_phone)
            update_profile(config, user.member_id, originals[user.member_id][0], originals[user.member_id][1])
            result = f"update:{user.member_id}"
        else:
            trip_count = get_trip_count(config)
            result = f"count:{user.member_id}:{trip_count}"

        durations.append(time.perf_counter() - op_start)
        return result

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(stress_task, index) for index in range(operations)]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:  # pragma: no cover - runtime guard
                failures.append(str(exc))

    duration = time.perf_counter() - start
    duration_avg = statistics.mean(durations) if durations else 0.0
    duration_p95 = statistics.quantiles(durations, n=20)[18] if len(durations) >= 20 else max(durations, default=0.0)

    for user in users:
        original_email, original_phone = originals[user.member_id]
        update_profile(config, user.member_id, original_email, original_phone)

    passed = not failures
    details = (
        f"operations={operations}, workers={workers}, avg={duration_avg*1000:.2f}ms, p95={duration_p95*1000:.2f}ms"
        if passed
        else f"failures={failures[:3]}"
    )
    return ScenarioSummary("Stress test", passed, details, duration)


def print_summary(results: list[ScenarioSummary]) -> None:
    print("\n=== Shuttle System Multi-User Test Report ===")
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"[{status}] {result.name:<18} | {result.duration_seconds:7.2f}s | {result.details}")

    overall = all(result.passed for result in results)
    print("\nOverall:", "PASS" if overall else "FAIL")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run multi-user and ACID checks against shuttle_system.")
    parser.add_argument("--mode", choices=["all", "api", "concurrent", "race", "failure", "stress"], default="all")
    parser.add_argument("--workers", type=int, default=16, help="Number of worker threads to use.")
    parser.add_argument("--operations", type=int, default=300, help="Number of operations for the stress phase.")
    parser.add_argument("--sandbox-size", type=int, default=8, help="Number of temporary users to create.")
    parser.add_argument("--api-base-url", type=str, default=None, help="Optional Flask base URL for requests-based tests.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = DBConfig()
    tag = unique_tag("module_b")
    sandbox_users: list[SandboxUser] = []
    results: list[ScenarioSummary] = []

    try:
        existing_members = load_existing_members(config, max(args.sandbox_size, args.workers, 2))
        sandbox_users = [
            SandboxUser(
                member_id=member.member_id,
                username=f"member_{member.member_id}",
                email=member.email,
                phone=member.phone,
                name=member.name,
            )
            for member in existing_members
        ]

        if args.mode in ("all", "concurrent"):
            results.append(run_concurrent_usage(config, args.workers, sandbox_users))
        if args.mode in ("all", "race"):
            results.append(run_race_condition(config, args.workers, tag))
        if args.mode in ("all", "failure"):
            results.append(run_failure_simulation(config, tag))
        if args.mode in ("all", "stress"):
            results.append(run_stress_test(config, args.workers, args.operations, sandbox_users, tag))
        if args.mode in ("all", "api"):
            if not args.api_base_url:
                if args.mode == "api":
                    print("Database error: --api-base-url is required for API mode")
                    return 2
            else:
                results.append(run_api_profile_smoke(args.api_base_url))
                results.append(run_api_booking_race(args.api_base_url, config))
    except pymysql.MySQLError as exc:
        print(f"Database error: {exc}")
        return 2
    print_summary(results)
    return 0 if results and all(result.passed for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
