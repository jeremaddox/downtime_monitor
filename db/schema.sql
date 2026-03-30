-- =============================================================================
-- Downtime Monitor & Actuarial Loss Modeling System
-- Schema: PostgreSQL
-- =============================================================================

-- ---------------------------------------------------------------------------
-- TARGETS
-- Devices/services being monitored (servers, switches, APs, cameras, POS, etc.)
-- ---------------------------------------------------------------------------
CREATE TABLE targets (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    hostname        TEXT,
    ip_address      INET,
    fqdn            TEXT,
    target_type     TEXT NOT NULL,           -- 'server','switch','ap','camera','pos','printer','service', etc.
    location        TEXT,                    -- physical location (building, room)
    department      TEXT,
    priority        SMALLINT NOT NULL DEFAULT 3,  -- 1=critical, 2=high, 3=normal, 4=low
    check_protocol  TEXT NOT NULL DEFAULT 'icmp',  -- 'icmp','tcp','http','snmp','agent'
    check_port      INTEGER,
    check_path      TEXT,                    -- for HTTP checks (e.g. '/health')
    check_interval  INTEGER NOT NULL DEFAULT 60,   -- seconds between checks
    check_timeout   INTEGER NOT NULL DEFAULT 10,   -- seconds before timeout
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    tags            TEXT[],
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_targets_type       ON targets (target_type);
CREATE INDEX idx_targets_enabled    ON targets (enabled);
CREATE INDEX idx_targets_priority   ON targets (priority);
CREATE INDEX idx_targets_ip         ON targets (ip_address);

-- ---------------------------------------------------------------------------
-- RAW_CHECKS
-- Every individual probe result for every target.
-- High-volume; partition by month in production.
-- ---------------------------------------------------------------------------
CREATE TABLE raw_checks (
    id              BIGSERIAL PRIMARY KEY,
    target_id       INTEGER NOT NULL REFERENCES targets (id) ON DELETE CASCADE,
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    success         BOOLEAN NOT NULL,
    latency_ms      NUMERIC(10,3),           -- NULL on failure
    status_code     INTEGER,                 -- HTTP status or ICMP code
    error_message   TEXT,
    collector_id    TEXT                     -- which collector node ran this check
);

CREATE INDEX idx_raw_checks_target_time ON raw_checks (target_id, checked_at DESC);
CREATE INDEX idx_raw_checks_time        ON raw_checks (checked_at DESC);
CREATE INDEX idx_raw_checks_failure     ON raw_checks (target_id, checked_at DESC) WHERE success = FALSE;

-- ---------------------------------------------------------------------------
-- TARGET_STATE
-- Current / most-recent computed state for each target.
-- One row per target; upserted by the engine after each check.
-- ---------------------------------------------------------------------------
CREATE TABLE target_state (
    target_id           INTEGER PRIMARY KEY REFERENCES targets (id) ON DELETE CASCADE,
    status              TEXT NOT NULL DEFAULT 'unknown',  -- 'up','down','degraded','unknown'
    last_check_at       TIMESTAMPTZ,
    last_success_at     TIMESTAMPTZ,
    last_failure_at     TIMESTAMPTZ,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    consecutive_successes INTEGER NOT NULL DEFAULT 0,
    current_latency_ms  NUMERIC(10,3),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_target_state_status ON target_state (status);

-- ---------------------------------------------------------------------------
-- INCIDENTS
-- A continuous downtime period for a target (open until resolved).
-- ---------------------------------------------------------------------------
CREATE TABLE incidents (
    id              BIGSERIAL PRIMARY KEY,
    target_id       INTEGER NOT NULL REFERENCES targets (id) ON DELETE CASCADE,
    started_at      TIMESTAMPTZ NOT NULL,
    resolved_at     TIMESTAMPTZ,             -- NULL = still open
    duration_secs   INTEGER                  -- computed on close: EXTRACT(EPOCH FROM resolved_at - started_at)
                        GENERATED ALWAYS AS (
                            CASE WHEN resolved_at IS NOT NULL
                                 THEN EXTRACT(EPOCH FROM (resolved_at - started_at))::INTEGER
                            END
                        ) STORED,
    severity        TEXT NOT NULL DEFAULT 'normal',  -- 'critical','high','normal','low'
    cause           TEXT,                    -- root-cause classification
    notes           TEXT,
    acknowledged    BOOLEAN NOT NULL DEFAULT FALSE,
    acknowledged_by TEXT,
    acknowledged_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_incidents_target       ON incidents (target_id);
CREATE INDEX idx_incidents_open         ON incidents (target_id) WHERE resolved_at IS NULL;
CREATE INDEX idx_incidents_started      ON incidents (started_at DESC);
CREATE INDEX idx_incidents_severity     ON incidents (severity);

-- ---------------------------------------------------------------------------
-- TICKETS
-- Help-desk / work-order tickets linked to incidents.
-- ---------------------------------------------------------------------------
CREATE TABLE tickets (
    id              SERIAL PRIMARY KEY,
    incident_id     BIGINT REFERENCES incidents (id) ON DELETE SET NULL,
    target_id       INTEGER REFERENCES targets (id) ON DELETE SET NULL,
    external_id     TEXT,                    -- ticket number in external system (e.g. ServiceNow, Jira)
    external_system TEXT,                    -- 'servicenow','jira','freshdesk', etc.
    title           TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'open',   -- 'open','in_progress','resolved','closed'
    priority        SMALLINT NOT NULL DEFAULT 3,
    assigned_to     TEXT,
    opened_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ,
    resolution_notes TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tickets_incident   ON tickets (incident_id);
CREATE INDEX idx_tickets_target     ON tickets (target_id);
CREATE INDEX idx_tickets_status     ON tickets (status);
CREATE INDEX idx_tickets_external   ON tickets (external_system, external_id);

-- ---------------------------------------------------------------------------
-- LOSS_PARAMETERS
-- Configurable financial/actuarial parameters used in loss modeling.
-- Rows are versioned with effective_from/effective_to for audit history.
-- ---------------------------------------------------------------------------
CREATE TABLE loss_parameters (
    id                      SERIAL PRIMARY KEY,
    param_scope             TEXT NOT NULL DEFAULT 'global',  -- 'global','department','target_type','target'
    scope_ref               TEXT,            -- e.g. department name, target_type value, or target id
    param_key               TEXT NOT NULL,   -- e.g. 'revenue_per_hour','labor_cost_per_hour','sla_penalty_per_min'
    param_value             NUMERIC(18,4) NOT NULL,
    currency                CHAR(3) NOT NULL DEFAULT 'USD',
    description             TEXT,
    effective_from          DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_to            DATE,            -- NULL = currently active
    created_by              TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_loss_params_active ON loss_parameters (param_scope, COALESCE(scope_ref, ''), param_key)
    WHERE effective_to IS NULL;
CREATE INDEX idx_loss_params_scope ON loss_parameters (param_scope, scope_ref);

-- ---------------------------------------------------------------------------
-- ASSETS
-- Physical hardware assets tracked for lifecycle and warranty management.
-- ---------------------------------------------------------------------------
CREATE TABLE assets (
    id              SERIAL PRIMARY KEY,
    target_id       INTEGER REFERENCES targets (id) ON DELETE SET NULL,
    asset_tag       TEXT UNIQUE,
    serial_number   TEXT,
    manufacturer    TEXT,
    model           TEXT,
    asset_type      TEXT NOT NULL,           -- 'server','switch','ap','camera','ups','workstation', etc.
    location        TEXT,
    department      TEXT,
    purchase_date   DATE,
    purchase_price  NUMERIC(12,2),
    currency        CHAR(3) NOT NULL DEFAULT 'USD',
    useful_life_years SMALLINT,              -- for depreciation
    salvage_value   NUMERIC(12,2),
    status          TEXT NOT NULL DEFAULT 'active',  -- 'active','retired','spare','disposed'
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_assets_target      ON assets (target_id);
CREATE INDEX idx_assets_type        ON assets (asset_type);
CREATE INDEX idx_assets_status      ON assets (status);
CREATE INDEX idx_assets_serial      ON assets (serial_number);

-- ---------------------------------------------------------------------------
-- ASSET_EVENTS
-- Lifecycle events for assets: repairs, replacements, moves, disposals, etc.
-- ---------------------------------------------------------------------------
CREATE TABLE asset_events (
    id              BIGSERIAL PRIMARY KEY,
    asset_id        INTEGER NOT NULL REFERENCES assets (id) ON DELETE CASCADE,
    event_type      TEXT NOT NULL,           -- 'repair','replacement','move','upgrade','disposal','inspection'
    event_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    cost            NUMERIC(12,2),
    currency        CHAR(3) NOT NULL DEFAULT 'USD',
    performed_by    TEXT,
    vendor          TEXT,
    description     TEXT,
    incident_id     BIGINT REFERENCES incidents (id) ON DELETE SET NULL,
    ticket_id       INTEGER REFERENCES tickets (id) ON DELETE SET NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_asset_events_asset     ON asset_events (asset_id);
CREATE INDEX idx_asset_events_type      ON asset_events (event_type);
CREATE INDEX idx_asset_events_date      ON asset_events (event_date DESC);
CREATE INDEX idx_asset_events_incident  ON asset_events (incident_id);

-- ---------------------------------------------------------------------------
-- ASSET_PARAMETERS
-- Per-asset actuarial / financial parameters (override loss_parameters).
-- e.g. specific MTTR, failure rate, replacement cost for a given asset class.
-- ---------------------------------------------------------------------------
CREATE TABLE asset_parameters (
    id              SERIAL PRIMARY KEY,
    asset_id        INTEGER REFERENCES assets (id) ON DELETE CASCADE,  -- NULL = applies to asset_type
    asset_type      TEXT,                    -- used when asset_id IS NULL (type-level default)
    param_key       TEXT NOT NULL,           -- 'mttr_hours','failure_rate_annual','replacement_cost'
    param_value     NUMERIC(18,4) NOT NULL,
    unit            TEXT,                    -- 'hours','USD','count/year', etc.
    description     TEXT,
    effective_from  DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_to    DATE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_asset_or_type CHECK (
        (asset_id IS NOT NULL AND asset_type IS NULL) OR
        (asset_id IS NULL AND asset_type IS NOT NULL)
    )
);

CREATE INDEX idx_asset_params_asset     ON asset_parameters (asset_id);
CREATE INDEX idx_asset_params_type      ON asset_parameters (asset_type);

-- ---------------------------------------------------------------------------
-- WARRANTY_EVENTS
-- Warranty registrations, claims, and expirations for assets.
-- ---------------------------------------------------------------------------
CREATE TABLE warranty_events (
    id              SERIAL PRIMARY KEY,
    asset_id        INTEGER NOT NULL REFERENCES assets (id) ON DELETE CASCADE,
    event_type      TEXT NOT NULL,           -- 'registration','claim','extension','expiration'
    warranty_type   TEXT,                    -- 'manufacturer','extended','on-site','depot'
    vendor          TEXT,
    contract_number TEXT,
    start_date      DATE,
    end_date        DATE,
    claim_date      DATE,                    -- for 'claim' events
    claim_amount    NUMERIC(12,2),
    currency        CHAR(3) NOT NULL DEFAULT 'USD',
    outcome         TEXT,                    -- 'approved','denied','partial','pending'
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_warranty_events_asset      ON warranty_events (asset_id);
CREATE INDEX idx_warranty_events_type       ON warranty_events (event_type);
CREATE INDEX idx_warranty_events_end_date   ON warranty_events (end_date);

-- ---------------------------------------------------------------------------
-- VIEWS
-- ---------------------------------------------------------------------------

-- Current open incidents with target info
CREATE VIEW v_open_incidents AS
SELECT
    i.id            AS incident_id,
    t.name          AS target_name,
    t.target_type,
    t.location,
    t.priority,
    i.started_at,
    EXTRACT(EPOCH FROM (NOW() - i.started_at))::INTEGER AS duration_secs,
    i.severity,
    i.acknowledged
FROM incidents i
JOIN targets t ON t.id = i.target_id
WHERE i.resolved_at IS NULL;

-- Asset warranty status
CREATE VIEW v_asset_warranty_status AS
SELECT
    a.id            AS asset_id,
    a.asset_tag,
    a.manufacturer,
    a.model,
    a.asset_type,
    a.location,
    w.warranty_type,
    w.vendor,
    w.contract_number,
    w.end_date      AS warranty_end,
    CASE
        WHEN w.end_date < CURRENT_DATE           THEN 'expired'
        WHEN w.end_date < CURRENT_DATE + 90      THEN 'expiring_soon'
        ELSE                                          'active'
    END             AS warranty_status,
    w.end_date - CURRENT_DATE AS days_remaining
FROM assets a
LEFT JOIN LATERAL (
    SELECT *
    FROM warranty_events we
    WHERE we.asset_id = a.id
      AND we.event_type = 'registration'
    ORDER BY we.end_date DESC
    LIMIT 1
) w ON TRUE
WHERE a.status = 'active';

-- Daily incident summary per target
CREATE VIEW v_daily_incident_summary AS
SELECT
    t.id            AS target_id,
    t.name          AS target_name,
    t.target_type,
    DATE(i.started_at) AS incident_date,
    COUNT(*)        AS incident_count,
    SUM(COALESCE(i.duration_secs, 0)) AS total_downtime_secs,
    AVG(COALESCE(i.duration_secs, 0)) AS avg_duration_secs
FROM incidents i
JOIN targets t ON t.id = i.target_id
WHERE i.resolved_at IS NOT NULL
GROUP BY t.id, t.name, t.target_type, DATE(i.started_at);
