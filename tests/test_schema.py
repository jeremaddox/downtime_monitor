"""
Schema smoke tests + seed/query round-trip tests for downtime_monitor.

Part 1 - Schema: verifies all tables exist, required columns are present,
         and key constraints (NOT NULL, UNIQUE, CHECK) are enforced.
Part 2 - Data:   inserts a realistic slice of resort IT data, then validates
         that the three views return correct, computed results.

Run with:
    pytest tests/test_schema.py -v
"""

import datetime
import pytest
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Connection config
# ---------------------------------------------------------------------------
DB_DSN = "dbname=downtime_monitor"

EXPECTED_TABLES = [
    "targets",
    "raw_checks",
    "target_state",
    "incidents",
    "tickets",
    "loss_parameters",
    "assets",
    "asset_events",
    "asset_parameters",
    "warranty_events",
]

EXPECTED_VIEWS = [
    "v_open_incidents",
    "v_asset_warranty_status",
    "v_daily_incident_summary",
]


@pytest.fixture(scope="module")
def conn():
    """Module-scoped connection. Each test rolls back via savepoint."""
    c = psycopg2.connect(DB_DSN)
    c.autocommit = False
    yield c
    c.close()


@pytest.fixture()
def cur(conn):
    """Per-test cursor inside a savepoint — fully isolated, nothing persists."""
    with conn.cursor() as setup:
        setup.execute("SAVEPOINT test_start")
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
        yield c
    with conn.cursor() as teardown:
        teardown.execute("ROLLBACK TO SAVEPOINT test_start")


# ===========================================================================
# PART 1 - SCHEMA SMOKE TESTS
# ===========================================================================

class TestSchema:

    def test_all_tables_exist(self, cur):
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """)
        found = {row["table_name"] for row in cur.fetchall()}
        missing = set(EXPECTED_TABLES) - found
        assert not missing, f"Missing tables: {missing}"

    def test_all_views_exist(self, cur):
        cur.execute("""
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = 'public'
        """)
        found = {row["table_name"] for row in cur.fetchall()}
        missing = set(EXPECTED_VIEWS) - found
        assert not missing, f"Missing views: {missing}"

    # --- targets ------------------------------------------------------------

    def test_targets_required_columns(self, cur):
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'targets' AND table_schema = 'public'
        """)
        cols = {row["column_name"] for row in cur.fetchall()}
        required = {"id", "name", "target_type", "check_protocol",
                    "check_interval", "check_timeout", "enabled", "priority"}
        assert required <= cols

    def test_targets_name_not_null(self, cur):
        with pytest.raises(psycopg2.errors.NotNullViolation):
            cur.execute("INSERT INTO targets (target_type) VALUES ('server')")

    def test_targets_priority_default(self, cur):
        cur.execute("""
            INSERT INTO targets (name, target_type)
            VALUES ('test-srv', 'server') RETURNING priority
        """)
        assert cur.fetchone()["priority"] == 3

    # --- raw_checks ---------------------------------------------------------

    def test_raw_checks_requires_target_id(self, cur):
        with pytest.raises(psycopg2.errors.NotNullViolation):
            cur.execute("INSERT INTO raw_checks (success) VALUES (true)")

    def test_raw_checks_fk_enforced(self, cur):
        with pytest.raises(psycopg2.errors.ForeignKeyViolation):
            cur.execute("""
                INSERT INTO raw_checks (target_id, success) VALUES (999999, true)
            """)

    # --- incidents ----------------------------------------------------------

    def test_incidents_duration_generated(self, cur):
        cur.execute("""
            INSERT INTO targets (name, target_type)
            VALUES ('dur-test', 'server') RETURNING id
        """)
        tid = cur.fetchone()["id"]
        started  = datetime.datetime(2026, 1, 1, 10,  0, 0, tzinfo=datetime.timezone.utc)
        resolved = datetime.datetime(2026, 1, 1, 10, 30, 0, tzinfo=datetime.timezone.utc)
        cur.execute("""
            INSERT INTO incidents (target_id, started_at, resolved_at)
            VALUES (%s, %s, %s) RETURNING duration_secs
        """, (tid, started, resolved))
        assert cur.fetchone()["duration_secs"] == 1800

    def test_incidents_open_has_null_resolved(self, cur):
        cur.execute("""
            INSERT INTO targets (name, target_type)
            VALUES ('open-inc', 'server') RETURNING id
        """)
        tid = cur.fetchone()["id"]
        cur.execute("""
            INSERT INTO incidents (target_id, started_at)
            VALUES (%s, NOW()) RETURNING resolved_at, duration_secs
        """, (tid,))
        row = cur.fetchone()
        assert row["resolved_at"] is None
        assert row["duration_secs"] is None

    # --- loss_parameters ----------------------------------------------------

    def test_loss_parameters_unique_active(self, cur):
        cur.execute("""
            INSERT INTO loss_parameters (param_scope, param_key, param_value)
            VALUES ('global', 'revenue_per_hour', 5000)
        """)
        with pytest.raises(psycopg2.errors.UniqueViolation):
            cur.execute("""
                INSERT INTO loss_parameters (param_scope, param_key, param_value)
                VALUES ('global', 'revenue_per_hour', 9999)
            """)

    # --- asset_parameters ---------------------------------------------------

    def test_asset_parameters_check_constraint(self, cur):
        """Inserting with neither asset_id nor asset_type must fail."""
        with pytest.raises(psycopg2.errors.CheckViolation):
            cur.execute("""
                INSERT INTO asset_parameters (param_key, param_value)
                VALUES ('mttr_hours', 4)
            """)

    def test_asset_parameters_type_level_allowed(self, cur):
        cur.execute("""
            INSERT INTO asset_parameters (asset_type, param_key, param_value, unit)
            VALUES ('switch', 'mttr_hours', 4, 'hours') RETURNING id
        """)
        assert cur.fetchone()["id"] is not None


# ===========================================================================
# PART 2 - SEED + QUERY ROUND-TRIP TESTS
# ===========================================================================

class TestDataRoundTrip:
    """
    Inserts a realistic resort IT dataset and validates views and queries.
    The cur fixture rolls everything back after each test.
    """

    @pytest.fixture()
    def seed(self, cur):
        # --- targets --------------------------------------------------------
        cur.execute("""
            INSERT INTO targets (name, hostname, ip_address, target_type,
                                 location, department, priority, check_protocol)
            VALUES
              ('POS-Terminal-Lobby', 'pos-lobby-01', '10.1.1.10', 'pos',    'Lobby',       'F&B',      1, 'tcp'),
              ('Core-Switch-Main',   'sw-core-01',   '10.1.1.1',  'switch', 'Server Room', 'IT',       1, 'icmp'),
              ('AP-Pool-Deck',       'ap-pool-01',   '10.1.2.50', 'ap',     'Pool Deck',   'IT',       2, 'icmp'),
              ('Security-Camera-01', 'cam-entry-01', '10.1.3.10', 'camera', 'Main Entry',  'Security', 3, 'tcp')
            RETURNING id, name
        """)
        targets = {row["name"]: row["id"] for row in cur.fetchall()}

        # --- target_state ---------------------------------------------------
        for tid in targets.values():
            cur.execute("""
                INSERT INTO target_state (target_id, status, last_check_at,
                                          last_success_at, consecutive_failures)
                VALUES (%s, 'up', NOW(), NOW(), 0)
            """, (tid,))

        # --- raw_checks (5 recent checks for POS) ---------------------------
        pos_id = targets["POS-Terminal-Lobby"]
        for i in range(5):
            cur.execute("""
                INSERT INTO raw_checks (target_id, checked_at, success, latency_ms)
                VALUES (%s, NOW() - (%s || ' minutes')::INTERVAL, true, %s)
            """, (pos_id, i * 5, 12.5 + i))

        # --- incidents (one closed 45 min, one open 10 min) -----------------
        sw_id = targets["Core-Switch-Main"]
        closed_start = datetime.datetime(2026, 3, 28, 12,  0, tzinfo=datetime.timezone.utc)
        closed_end   = datetime.datetime(2026, 3, 28, 12, 45, tzinfo=datetime.timezone.utc)
        cur.execute("""
            INSERT INTO incidents (target_id, started_at, resolved_at, severity)
            VALUES (%s, %s, %s, 'high') RETURNING id
        """, (sw_id, closed_start, closed_end))
        closed_incident_id = cur.fetchone()["id"]

        cur.execute("""
            INSERT INTO incidents (target_id, started_at, severity)
            VALUES (%s, NOW() - INTERVAL '10 minutes', 'critical') RETURNING id
        """, (pos_id,))
        open_incident_id = cur.fetchone()["id"]

        # --- ticket ---------------------------------------------------------
        cur.execute("""
            INSERT INTO tickets (incident_id, target_id, title, status, priority)
            VALUES (%s, %s, 'POS terminal down - Lobby', 'open', 1)
        """, (open_incident_id, pos_id))

        # --- loss_parameters ------------------------------------------------
        cur.execute("""
            INSERT INTO loss_parameters (param_scope, param_key, param_value, description)
            VALUES
              ('global', 'revenue_per_hour',    8500, 'Resort avg revenue/hr'),
              ('global', 'labor_cost_per_hour',  125, 'Avg IT labor cost/hr'),
              ('global', 'sla_penalty_per_min',   50, 'SLA penalty per minute')
        """)

        # --- assets ---------------------------------------------------------
        cur.execute("""
            INSERT INTO assets (target_id, asset_tag, serial_number, manufacturer,
                                model, asset_type, purchase_date, purchase_price,
                                useful_life_years, salvage_value)
            VALUES
              (%s, 'AST-001', 'SN-SW-001',  'Cisco',    'Catalyst 9300', 'switch',
               '2022-06-01', 12500.00, 7, 500.00),
              (%s, 'AST-002', 'SN-POS-001', 'Verifone', 'P630',          'pos',
               '2024-01-15',   850.00, 5,  50.00)
            RETURNING id, asset_tag
        """, (sw_id, pos_id))
        assets = {row["asset_tag"]: row["id"] for row in cur.fetchall()}

        # --- asset_events ---------------------------------------------------
        cur.execute("""
            INSERT INTO asset_events (asset_id, event_type, event_date, cost,
                                      vendor, description, incident_id)
            VALUES (%s, 'repair', '2026-03-28', 250.00, 'Cisco TAC',
                    'Fan replacement after thermal shutdown', %s)
        """, (assets["AST-001"], closed_incident_id))

        # --- asset_parameters -----------------------------------------------
        cur.execute("""
            INSERT INTO asset_parameters (asset_type, param_key, param_value, unit)
            VALUES
              ('switch', 'mttr_hours',           4,   'hours'),
              ('switch', 'failure_rate_annual', 0.15, 'failures/year'),
              ('pos',    'mttr_hours',           2,   'hours'),
              ('pos',    'replacement_cost',    900,  'USD')
        """)

        # --- warranty_events ------------------------------------------------
        sw_asset  = assets["AST-001"]
        pos_asset = assets["AST-002"]
        cur.execute("""
            INSERT INTO warranty_events (asset_id, event_type, warranty_type,
                                         vendor, start_date, end_date)
            VALUES
              (%s, 'registration', 'manufacturer', 'Cisco',    '2022-06-01', '2025-06-01'),
              (%s, 'registration', 'extended',     'CDW',      '2025-06-01', '2027-06-01'),
              (%s, 'registration', 'manufacturer', 'Verifone', '2024-01-15', '2026-01-15')
        """, (sw_asset, sw_asset, pos_asset))

        return {
            "targets": targets,
            "assets": assets,
            "open_incident_id": open_incident_id,
            "closed_incident_id": closed_incident_id,
        }

    # --- raw_checks ---------------------------------------------------------

    def test_raw_checks_inserted(self, cur, seed):
        pos_id = seed["targets"]["POS-Terminal-Lobby"]
        cur.execute("SELECT COUNT(*) AS n FROM raw_checks WHERE target_id = %s", (pos_id,))
        assert cur.fetchone()["n"] == 5

    # --- incidents ----------------------------------------------------------

    def test_closed_incident_duration(self, cur, seed):
        cur.execute("SELECT duration_secs FROM incidents WHERE id = %s",
                    (seed["closed_incident_id"],))
        assert cur.fetchone()["duration_secs"] == 2700  # 45 min

    # --- v_open_incidents ---------------------------------------------------

    def test_view_open_incidents_contains_pos(self, cur, seed):
        cur.execute("SELECT target_name FROM v_open_incidents")
        names = [r["target_name"] for r in cur.fetchall()]
        assert "POS-Terminal-Lobby" in names

    def test_view_open_incidents_excludes_closed(self, cur, seed):
        cur.execute("SELECT incident_id FROM v_open_incidents")
        ids = [r["incident_id"] for r in cur.fetchall()]
        assert seed["closed_incident_id"] not in ids

    def test_view_open_incidents_duration_positive(self, cur, seed):
        cur.execute("""
            SELECT duration_secs FROM v_open_incidents WHERE incident_id = %s
        """, (seed["open_incident_id"],))
        row = cur.fetchone()
        assert row is not None
        assert row["duration_secs"] > 0

    # --- v_asset_warranty_status --------------------------------------------

    def test_warranty_active_for_switch(self, cur, seed):
        """Extended warranty ends 2027-06-01 — should be 'active'."""
        cur.execute("""
            SELECT warranty_status FROM v_asset_warranty_status WHERE asset_id = %s
        """, (seed["assets"]["AST-001"],))
        assert cur.fetchone()["warranty_status"] == "active"

    def test_warranty_expired_for_pos(self, cur, seed):
        """POS warranty ended 2026-01-15, before today 2026-03-30 — should be 'expired'."""
        cur.execute("""
            SELECT warranty_status FROM v_asset_warranty_status WHERE asset_id = %s
        """, (seed["assets"]["AST-002"],))
        assert cur.fetchone()["warranty_status"] == "expired"

    # --- v_daily_incident_summary -------------------------------------------

    def test_daily_summary_counts_closed_incident(self, cur, seed):
        sw_id = seed["targets"]["Core-Switch-Main"]
        cur.execute("""
            SELECT incident_count, total_downtime_secs
            FROM v_daily_incident_summary
            WHERE target_id = %s AND incident_date = '2026-03-28'
        """, (sw_id,))
        row = cur.fetchone()
        assert row is not None
        assert row["incident_count"] == 1
        assert row["total_downtime_secs"] == 2700

    def test_daily_summary_excludes_open_incidents(self, cur, seed):
        """Open incidents must not appear in daily summary."""
        pos_id = seed["targets"]["POS-Terminal-Lobby"]
        cur.execute("""
            SELECT COUNT(*) AS n FROM v_daily_incident_summary
            WHERE target_id = %s AND incident_date = CURRENT_DATE
        """, (pos_id,))
        assert cur.fetchone()["n"] == 0

    # --- loss_parameters ----------------------------------------------------

    def test_loss_parameters_revenue_retrievable(self, cur, seed):
        cur.execute("""
            SELECT param_value FROM loss_parameters
            WHERE param_scope = 'global' AND param_key = 'revenue_per_hour'
              AND effective_to IS NULL
        """)
        assert cur.fetchone()["param_value"] == 8500

    # --- asset_events -------------------------------------------------------

    def test_asset_event_linked_to_incident(self, cur, seed):
        cur.execute("""
            SELECT incident_id, cost FROM asset_events
            WHERE asset_id = %s AND event_type = 'repair'
        """, (seed["assets"]["AST-001"],))
        row = cur.fetchone()
        assert row["incident_id"] == seed["closed_incident_id"]
        assert row["cost"] == 250.00
