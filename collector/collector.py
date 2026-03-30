"""
Main collector loop.

- Reads enabled targets from the database.
- Dispatches checks concurrently using a thread pool.
- Writes every result to raw_checks.
- Upserts target_state after each check.
- Sleeps until the next check is due.

Run:
    python3 -m collector.collector
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

from collector.config import DB_DSN, COLLECTOR_ID, THREAD_POOL_SIZE
from collector.checks import icmp, tcp, http

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_due_targets(cur):
    """Return targets whose next check is due now."""
    cur.execute("""
        SELECT
            t.id, t.name, t.hostname, t.ip_address::text AS ip_address,
            t.target_type, t.check_protocol, t.check_port, t.check_path,
            t.check_interval, t.check_timeout,
            ts.last_check_at
        FROM targets t
        LEFT JOIN target_state ts ON ts.target_id = t.id
        WHERE t.enabled = TRUE
          AND (
              ts.last_check_at IS NULL
              OR ts.last_check_at + (t.check_interval || ' seconds')::INTERVAL <= NOW()
          )
        ORDER BY t.priority, t.id
    """)
    return cur.fetchall()


def write_raw_check(cur, target_id, result):
    cur.execute("""
        INSERT INTO raw_checks
            (target_id, checked_at, success, latency_ms, status_code, error_message, collector_id)
        VALUES
            (%s, NOW(), %s, %s, %s, %s, %s)
    """, (
        target_id,
        result["success"],
        result.get("latency_ms"),
        result.get("status_code"),
        result.get("error"),
        COLLECTOR_ID,
    ))


def upsert_target_state(cur, target_id, result):
    cur.execute("""
        INSERT INTO target_state
            (target_id, status, last_check_at, last_success_at, last_failure_at,
             consecutive_failures, consecutive_successes, current_latency_ms, updated_at)
        VALUES (
            %(id)s,
            %(status)s,
            NOW(),
            CASE WHEN %(success)s THEN NOW() END,
            CASE WHEN NOT %(success)s THEN NOW() END,
            CASE WHEN %(success)s THEN 0 ELSE 1 END,
            CASE WHEN %(success)s THEN 1 ELSE 0 END,
            %(latency)s,
            NOW()
        )
        ON CONFLICT (target_id) DO UPDATE SET
            status               = EXCLUDED.status,
            last_check_at        = EXCLUDED.last_check_at,
            last_success_at      = COALESCE(EXCLUDED.last_success_at, target_state.last_success_at),
            last_failure_at      = COALESCE(EXCLUDED.last_failure_at, target_state.last_failure_at),
            consecutive_failures = CASE
                WHEN %(success)s THEN 0
                ELSE target_state.consecutive_failures + 1
            END,
            consecutive_successes = CASE
                WHEN %(success)s THEN target_state.consecutive_successes + 1
                ELSE 0
            END,
            current_latency_ms   = EXCLUDED.current_latency_ms,
            updated_at           = NOW()
    """, {
        "id":      target_id,
        "status":  "up" if result["success"] else "down",
        "success": result["success"],
        "latency": result.get("latency_ms"),
    })


# ---------------------------------------------------------------------------
# Check dispatcher
# ---------------------------------------------------------------------------

def run_check(target):
    """Run the appropriate check for a target. Returns (target, result)."""
    host    = (target["ip_address"] or "").split("/")[0] or target["hostname"]
    proto   = target["check_protocol"]
    port    = target["check_port"]
    path    = target["check_path"]
    timeout = target["check_timeout"]

    if proto == "icmp":
        result = icmp.check(host, timeout=timeout)
    elif proto == "tcp":
        result = tcp.check(host, port=port or 80, timeout=timeout)
    elif proto == "http":
        result = http.check(host, port=port, path=path, timeout=timeout)
    else:
        result = {"success": False, "latency_ms": None,
                  "error": f"unsupported protocol: {proto}"}

    return target, result


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run():
    log.info("Collector starting  [id=%s  pool=%s]", COLLECTOR_ID, THREAD_POOL_SIZE)

    while True:
        loop_start = time.monotonic()

        try:
            with psycopg2.connect(DB_DSN) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    targets = get_due_targets(cur)

            if not targets:
                time.sleep(5)
                continue

            log.info("Checking %d target(s)", len(targets))

            with ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE) as pool:
                futures = {pool.submit(run_check, t): t for t in targets}
                for future in as_completed(futures):
                    target, result = future.result()
                    status = "UP" if result["success"] else "DOWN"
                    latency = f"{result['latency_ms']:.1f}ms" if result.get("latency_ms") else "-"
                    log.info("%-30s  %-4s  %s  %s",
                             target["name"], status, latency,
                             result.get("error") or "")

                    with psycopg2.connect(DB_DSN) as conn:
                        with conn.cursor() as cur:
                            write_raw_check(cur, target["id"], result)
                            upsert_target_state(cur, target["id"], result)

        except psycopg2.OperationalError as e:
            log.error("DB connection error: %s", e)
            time.sleep(10)
            continue
        except Exception as e:
            log.exception("Unexpected error: %s", e)
            time.sleep(10)
            continue

        elapsed = time.monotonic() - loop_start
        sleep_for = max(0, 5 - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    run()
