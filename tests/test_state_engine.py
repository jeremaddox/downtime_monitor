"""
State engine tests.

Verifies:
  - Incident opens after hitting the failure threshold
  - Incident does NOT open before the threshold
  - Incident closes after recovery threshold successes
  - Priority 1 targets open faster (threshold=2) than normal (threshold=3)
  - Second incident opens if device goes down again after recovery
  - No duplicate open incidents for the same target
"""

import datetime
import pytest
import psycopg2
import psycopg2.extras

from engine.state_engine import process_target, FAILURE_THRESHOLD, RECOVERY_THRESHOLD

DB_DSN = "dbname=downtime_monitor"

NOW = datetime.datetime.now(datetime.timezone.utc)


def ts(offset_seconds):
    return NOW + datetime.timedelta(seconds=offset_seconds)


@pytest.fixture(scope="module")
def conn():
    c = psycopg2.connect(DB_DSN)
    c.autocommit = False
    yield c
    c.close()


@pytest.fixture()
def cur(conn):
    with conn.cursor() as setup:
        setup.execute("SAVEPOINT se_test")
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
        yield c
    with conn.cursor() as teardown:
        teardown.execute("ROLLBACK TO SAVEPOINT se_test")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_target(cur, name="test-device", priority=3):
    cur.execute("""
        INSERT INTO targets (name, target_type, priority, check_protocol)
        VALUES (%s, 'server', %s, 'icmp') RETURNING id
    """, (name, priority))
    tid = cur.fetchone()["id"]
    # Initial state row
    cur.execute("""
        INSERT INTO target_state (target_id, status, consecutive_failures,
                                   consecutive_successes)
        VALUES (%s, 'up', 0, 0)
    """, (tid,))
    return tid


def insert_check(cur, target_id, success, offset_seconds):
    cur.execute("""
        INSERT INTO raw_checks (target_id, checked_at, success, latency_ms)
        VALUES (%s, %s, %s, %s)
    """, (target_id, ts(offset_seconds), success, 5.0 if success else None))


def get_state(cur, target_id):
    cur.execute("""
        SELECT status, consecutive_failures, consecutive_successes
        FROM target_state WHERE target_id = %s
    """, (target_id,))
    return cur.fetchone()


def get_incidents(cur, target_id):
    cur.execute("""
        SELECT id, resolved_at, severity FROM incidents
        WHERE target_id = %s ORDER BY started_at
    """, (target_id,))
    return cur.fetchall()


def run_engine(cur, target_id):
    """Load target row and run process_target."""
    cur.execute("""
        SELECT t.id, t.name, t.priority,
               ts.status, ts.consecutive_failures,
               ts.consecutive_successes, ts.last_check_at
        FROM targets t
        LEFT JOIN target_state ts ON ts.target_id = t.id
        WHERE t.id = %s
    """, (target_id,))
    target = cur.fetchone()
    process_target(cur, target)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestStateEngine:

    def test_no_incident_below_threshold(self, cur):
        """2 failures for priority-3 target (threshold=3) should NOT open incident."""
        tid = make_target(cur, "below-thresh", priority=3)
        insert_check(cur, tid, False, 0)
        insert_check(cur, tid, False, 30)
        run_engine(cur, tid)
        assert get_incidents(cur, tid) == []

    def test_incident_opens_at_threshold(self, cur):
        """3 failures for priority-3 target should open incident."""
        tid = make_target(cur, "at-thresh", priority=3)
        for i in range(3):
            insert_check(cur, tid, False, i * 30)
        run_engine(cur, tid)
        incidents = get_incidents(cur, tid)
        assert len(incidents) == 1
        assert incidents[0]["resolved_at"] is None

    def test_priority1_opens_faster(self, cur):
        """Priority-1 target opens incident after only 2 failures."""
        tid = make_target(cur, "critical-device", priority=1)
        insert_check(cur, tid, False, 0)
        insert_check(cur, tid, False, 30)
        run_engine(cur, tid)
        incidents = get_incidents(cur, tid)
        assert len(incidents) == 1
        assert incidents[0]["severity"] == "critical"

    def test_incident_closes_after_recovery(self, cur):
        """3 successes after being down should close the open incident."""
        tid = make_target(cur, "recovers", priority=3)
        # Go down
        for i in range(3):
            insert_check(cur, tid, False, i * 30)
        run_engine(cur, tid)
        assert get_incidents(cur, tid)[0]["resolved_at"] is None

        # Recover — need to re-run engine with updated state
        for i in range(RECOVERY_THRESHOLD):
            insert_check(cur, tid, True, 120 + i * 30)
        run_engine(cur, tid)

        incidents = get_incidents(cur, tid)
        assert incidents[0]["resolved_at"] is not None

    def test_state_shows_down(self, cur):
        """Target state should be 'down' after incident opens."""
        tid = make_target(cur, "state-down", priority=3)
        for i in range(3):
            insert_check(cur, tid, False, i * 30)
        run_engine(cur, tid)
        assert get_state(cur, tid)["status"] == "down"

    def test_state_shows_up_after_recovery(self, cur):
        tid = make_target(cur, "state-up", priority=3)
        for i in range(3):
            insert_check(cur, tid, False, i * 30)
        run_engine(cur, tid)
        for i in range(RECOVERY_THRESHOLD):
            insert_check(cur, tid, True, 120 + i * 30)
        run_engine(cur, tid)
        assert get_state(cur, tid)["status"] == "up"

    def test_no_duplicate_incidents(self, cur):
        """Multiple engine runs while device is down should not create duplicate incidents."""
        tid = make_target(cur, "no-dupes", priority=3)
        for i in range(5):
            insert_check(cur, tid, False, i * 30)
        run_engine(cur, tid)
        run_engine(cur, tid)   # run again — should not open a second incident
        assert len(get_incidents(cur, tid)) == 1

    def test_second_incident_after_recovery(self, cur):
        """Device goes down, recovers, then goes down again — two separate incidents."""
        tid = make_target(cur, "two-incidents", priority=3)

        # First outage
        for i in range(3):
            insert_check(cur, tid, False, i * 30)
        run_engine(cur, tid)

        # Recovery
        for i in range(RECOVERY_THRESHOLD):
            insert_check(cur, tid, True, 120 + i * 30)
        run_engine(cur, tid)

        # Second outage
        for i in range(3):
            insert_check(cur, tid, False, 300 + i * 30)
        run_engine(cur, tid)

        incidents = get_incidents(cur, tid)
        assert len(incidents) == 2
        assert incidents[0]["resolved_at"] is not None   # first closed
        assert incidents[1]["resolved_at"] is None        # second still open
