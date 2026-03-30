"""
State Engine

Reads raw_checks written by the collector and:
  - Updates target_state (status, consecutive counts)
  - Opens an incident when a target hits FAILURE_THRESHOLD consecutive failures
  - Closes an incident when a target hits RECOVERY_THRESHOLD consecutive successes
  - Sets incident severity based on target priority

Run alongside the collector:
    python3 -m engine.state_engine

The engine processes only checks it hasn't seen yet (tracks a high-water mark
per target using target_state.last_check_at).
"""

import logging
import time

import psycopg2
import psycopg2.extras

from collector.config import DB_DSN

log = logging.getLogger(__name__)

# --- Thresholds -------------------------------------------------------------
# How many consecutive failures before opening an incident
FAILURE_THRESHOLD = {
    1: 2,   # priority 1 (critical) — open after 2 failures
    2: 3,   # priority 2 (high)
    3: 3,   # priority 3 (normal)
    4: 5,   # priority 4 (low)
}

# How many consecutive successes before closing an incident
RECOVERY_THRESHOLD = 3

POLL_INTERVAL = 10   # seconds between engine cycles

# --- Severity mapping -------------------------------------------------------
PRIORITY_TO_SEVERITY = {
    1: "critical",
    2: "high",
    3: "normal",
    4: "low",
}


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_targets(cur):
    cur.execute("""
        SELECT t.id, t.name, t.priority,
               ts.status,
               ts.consecutive_failures,
               ts.consecutive_successes,
               ts.last_check_at
        FROM targets t
        LEFT JOIN target_state ts ON ts.target_id = t.id
        WHERE t.enabled = TRUE
    """)
    return cur.fetchall()


def get_new_checks(cur, target_id, since):
    """Get checks for a target newer than `since`, oldest first."""
    if since is None:
        cur.execute("""
            SELECT id, checked_at, success, latency_ms, error_message
            FROM raw_checks
            WHERE target_id = %s
            ORDER BY checked_at ASC
        """, (target_id,))
    else:
        cur.execute("""
            SELECT id, checked_at, success, latency_ms, error_message
            FROM raw_checks
            WHERE target_id = %s AND checked_at > %s
            ORDER BY checked_at ASC
        """, (target_id, since))
    return cur.fetchall()


def get_open_incident(cur, target_id):
    cur.execute("""
        SELECT id FROM incidents
        WHERE target_id = %s AND resolved_at IS NULL
        ORDER BY started_at DESC
        LIMIT 1
    """, (target_id,))
    row = cur.fetchone()
    return row["id"] if row else None


def open_incident(cur, target_id, started_at, severity):
    cur.execute("""
        INSERT INTO incidents (target_id, started_at, severity)
        VALUES (%s, %s, %s)
        RETURNING id
    """, (target_id, started_at, severity))
    return cur.fetchone()["id"]


def close_incident(cur, incident_id, resolved_at):
    cur.execute("""
        UPDATE incidents
        SET resolved_at = %s
        WHERE id = %s
    """, (resolved_at, incident_id))


def update_target_state(cur, target_id, status, consec_fail, consec_ok,
                        last_check_at, latency_ms):
    cur.execute("""
        INSERT INTO target_state
            (target_id, status, last_check_at, consecutive_failures,
             consecutive_successes, current_latency_ms, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (target_id) DO UPDATE SET
            status                = EXCLUDED.status,
            last_check_at         = EXCLUDED.last_check_at,
            consecutive_failures  = EXCLUDED.consecutive_failures,
            consecutive_successes = EXCLUDED.consecutive_successes,
            current_latency_ms    = EXCLUDED.current_latency_ms,
            updated_at            = NOW()
    """, (target_id, status, last_check_at, consec_fail, consec_ok, latency_ms))


# ---------------------------------------------------------------------------
# Core logic — process one target
# ---------------------------------------------------------------------------

def process_target(cur, target):
    tid      = target["id"]
    name     = target["name"]
    priority = target["priority"] or 3

    fail_threshold = FAILURE_THRESHOLD.get(priority, 3)
    severity       = PRIORITY_TO_SEVERITY.get(priority, "normal")

    consec_fail = target["consecutive_failures"] or 0
    consec_ok   = target["consecutive_successes"] or 0
    status      = target["status"] or "unknown"
    since       = target["last_check_at"]

    checks = get_new_checks(cur, tid, since)
    if not checks:
        return

    for check in checks:
        if check["success"]:
            consec_fail  = 0
            consec_ok   += 1
        else:
            consec_ok    = 0
            consec_fail += 1

        # --- Transition: UP → DOWN
        if consec_fail >= fail_threshold and status != "down":
            incident_id = get_open_incident(cur, tid)
            if not incident_id:
                incident_id = open_incident(cur, tid, check["checked_at"], severity)
                log.warning("INCIDENT OPENED  %-30s  severity=%-8s  id=%s",
                            name, severity, incident_id)
            status = "down"

        # --- Transition: DOWN → UP
        elif consec_ok >= RECOVERY_THRESHOLD and status == "down":
            incident_id = get_open_incident(cur, tid)
            if incident_id:
                close_incident(cur, incident_id, check["checked_at"])
                log.info("INCIDENT CLOSED  %-30s  id=%s", name, incident_id)
            status = "up"

        elif consec_fail == 0 and status != "down":
            status = "up"

    # Persist updated state
    last_check = checks[-1]
    update_target_state(
        cur, tid, status,
        consec_fail, consec_ok,
        last_check["checked_at"],
        last_check["latency_ms"],
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run():
    log.info("State engine starting  [fail_thresholds=%s  recovery=%s]",
             FAILURE_THRESHOLD, RECOVERY_THRESHOLD)

    while True:
        try:
            with psycopg2.connect(DB_DSN) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    targets = get_targets(cur)
                    for target in targets:
                        process_target(cur, target)

        except psycopg2.OperationalError as e:
            log.error("DB error: %s", e)
            time.sleep(15)
        except Exception as e:
            log.exception("State engine error: %s", e)
            time.sleep(15)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run()
