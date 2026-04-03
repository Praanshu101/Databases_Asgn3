# CS432 Track 1 Submission (Assignment 3)

This repository contains the Assignment 2 base system plus Assignment 3 extensions for:
- Transaction management with `BEGIN/COMMIT/ROLLBACK`
- Write-ahead logging (WAL) and crash recovery
- ACID validation across three relations (`users`, `products`, `orders`)
- Multi-threaded concurrency and stress testing


Run ACID tests:

```powershell
cd Module_A
python acid_validation.py
```

Expected output includes:
- `Atomicity + Durability + Recovery PASS`
- `Isolation under contention PASS`


## Assignment 3 Stress Test (Module B)

Run multi-user concurrency and failure simulation:

```powershell
cd Module_B/app
python concurrency_stress_test.py
```

The script reports:
- Total requests, success/failure counts
- Throughput (requests per second)
- Recovery validation after simulated crash
- Consistency validation status
