-- =============================================================================
-- Weather tracking tables
-- Pine Mountain, GA  —  NWS station KPIM
-- =============================================================================

-- ---------------------------------------------------------------------------
-- WEATHER_OBSERVATIONS
-- Periodic snapshots from the NWS observation station.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_observations (
    id                  BIGSERIAL PRIMARY KEY,
    observed_at         TIMESTAMPTZ NOT NULL,
    station_id          TEXT NOT NULL DEFAULT 'KPIM',
    temp_f              NUMERIC(5,1),
    dewpoint_f          NUMERIC(5,1),
    humidity_pct        NUMERIC(5,1),
    wind_speed_mph      NUMERIC(5,1),
    wind_gust_mph       NUMERIC(5,1),
    wind_direction_deg  INTEGER,
    pressure_inhg       NUMERIC(6,2),
    precip_1h_in        NUMERIC(6,3),   -- precipitation last hour (inches)
    visibility_miles    NUMERIC(6,2),
    condition           TEXT,           -- e.g. 'Thunderstorm', 'Rain', 'Clear'
    raw_text            TEXT,           -- full text description from NWS
    UNIQUE (station_id, observed_at)
);

CREATE INDEX idx_wx_obs_time ON weather_observations (observed_at DESC);

-- ---------------------------------------------------------------------------
-- WEATHER_ALERTS
-- NWS active alerts for Harris County / Pine Mountain area.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_alerts (
    id              BIGSERIAL PRIMARY KEY,
    nws_id          TEXT UNIQUE NOT NULL,   -- NWS alert ID
    event_type      TEXT NOT NULL,          -- 'Tornado Warning', 'Severe Thunderstorm Warning', etc.
    severity        TEXT,                   -- 'Extreme','Severe','Moderate','Minor','Unknown'
    urgency         TEXT,                   -- 'Immediate','Expected','Future'
    headline        TEXT,
    description     TEXT,
    effective_at    TIMESTAMPTZ NOT NULL,
    expires_at      TIMESTAMPTZ,
    ended_at        TIMESTAMPTZ,            -- NULL = still active
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_wx_alerts_active  ON weather_alerts (expires_at) WHERE ended_at IS NULL;
CREATE INDEX idx_wx_alerts_time    ON weather_alerts (effective_at DESC);
CREATE INDEX idx_wx_alerts_type    ON weather_alerts (event_type);

-- ---------------------------------------------------------------------------
-- INCIDENT_WEATHER  (join table)
-- Links incidents to weather alerts active at the time the incident started.
-- Populated by the correlator engine.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS incident_weather (
    incident_id     BIGINT NOT NULL REFERENCES incidents (id) ON DELETE CASCADE,
    alert_id        BIGINT NOT NULL REFERENCES weather_alerts (id) ON DELETE CASCADE,
    PRIMARY KEY (incident_id, alert_id)
);

-- ---------------------------------------------------------------------------
-- VIEW: outages during weather events
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_weather_outage_correlation AS
SELECT
    wa.event_type,
    wa.severity,
    wa.effective_at,
    wa.expires_at,
    t.name          AS target_name,
    t.target_type,
    t.location,
    i.id            AS incident_id,
    i.started_at,
    i.duration_secs,
    i.severity      AS incident_severity
FROM incident_weather iw
JOIN weather_alerts wa  ON wa.id  = iw.alert_id
JOIN incidents i        ON i.id   = iw.incident_id
JOIN targets t          ON t.id   = i.target_id;
