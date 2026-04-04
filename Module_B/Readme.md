# Module B — Multi-User Behaviour and Stress Testing

This module includes a runnable database harness for the `shuttle_system` schema in `sql/01_shuttle_system.sql`.

## What it tests

- **Concurrent usage**: multiple users read and update profiles at the same time.
- **Race conditions**: many users try to book the same seat for the same trip; only one should commit.
- **Failure simulation**: an injected exception verifies rollback with no partial rows.
- **Stress testing**: hundreds of mixed read/write transactions run under load.
- **API smoke tests**: `requests` hits `/api/profile` and `/api/bookings` through Flask.

## Run

From the repository root:

```bash
python Module_B/concurrency_stress_test.py --mode all
```

Or run a single scenario:

```bash
python Module_B/concurrency_stress_test.py --mode race --workers 24
python Module_B/concurrency_stress_test.py --mode failure
python Module_B/concurrency_stress_test.py --mode stress --operations 1000 --workers 32
```

Run the Flask API tests after starting `Module_B/app/app.py`:

```bash
python Module_B/concurrency_stress_test.py --mode api --api-base-url http://127.0.0.1:8000
python Module_B/concurrency_stress_test.py --mode all --api-base-url http://127.0.0.1:8000
```

## Configuration

The script uses the same defaults as `Module_B/app/app.py`:

- `MYSQL_HOST=localhost`
- `MYSQL_PORT=3306`
- `MYSQL_USER=root`
- `MYSQL_PASSWORD=Samarth@05`
- `MYSQL_DATABASE=shuttle_system`

Override them with environment variables if needed.

In the current seeded database snapshot, `admin_rahul` uses `password123` and `user_ananya` uses `password1`.

## Output

The script prints a compact PASS/FAIL report plus timing for each scenario, including throughput and latency for stress runs.
