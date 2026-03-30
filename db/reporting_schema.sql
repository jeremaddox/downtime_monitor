-- =============================================================================
-- Reporting Schema — Power BI ready flat views
-- All views use simple types, human-readable column names, no arrays.
-- Connect Power BI directly to PostgreSQL using these views.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS reporting;

-- ---------------------------------------------------------------------------
-- rpt_incidents
-- One row per incident with all context Power BI needs for slicing.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW reporting.rpt_incidents AS
SELECT
    i.id                                        AS incident_id,
    t.name                                      AS device_name,
    t.target_type                               AS device_type,
    t.location                                  AS location,
    t.department                                AS department,
    t.priority                                  AS device_priority,
    CASE t.priority
        WHEN 1 THEN 'Critical'
        WHEN 2 THEN 'High'
        WHEN 3 THEN 'Normal'
        WHEN 4 THEN 'Low'
        ELSE 'Unknown'
    END                                         AS device_priority_label,
    i.severity                                  AS incident_severity,
    i.started_at                                AS started_at,
    i.resolved_at                               AS resolved_at,
    CASE WHEN i.resolved_at IS NULL
         THEN 'Open' ELSE 'Closed'
    END                                         AS incident_status,
    COALESCE(i.duration_secs, 0)                AS duration_seconds,
    ROUND(COALESCE(i.duration_secs, 0) / 60.0, 1)   AS duration_minutes,
    ROUND(COALESCE(i.duration_secs, 0) / 3600.0, 2)  AS duration_hours,
    DATE(i.started_at)                          AS incident_date,
    TO_CHAR(i.started_at, 'YYYY-MM')            AS incident_month,
    EXTRACT(YEAR  FROM i.started_at)::INTEGER   AS incident_year,
    EXTRACT(MONTH FROM i.started_at)::INTEGER   AS incident_month_num,
    EXTRACT(DOW   FROM i.started_at)::INTEGER   AS incident_day_of_week,
    TO_CHAR(i.started_at, 'Day')                AS incident_day_name,
    i.cause                                     AS root_cause,
    i.acknowledged                              AS acknowledged,
    i.notes                                     AS notes
FROM incidents i
JOIN targets t ON t.id = i.target_id;

-- ---------------------------------------------------------------------------
-- rpt_uptime_summary
-- Daily uptime % per device — the key SLA metric.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW reporting.rpt_uptime_summary AS
WITH day_series AS (
    SELECT
        t.id        AS target_id,
        t.name      AS device_name,
        t.target_type,
        t.location,
        t.department,
        t.priority,
        d.day
    FROM targets t
    CROSS JOIN (
        SELECT generate_series(
            DATE_TRUNC('day', NOW() - INTERVAL '90 days'),
            DATE_TRUNC('day', NOW()),
            '1 day'::INTERVAL
        )::DATE AS day
    ) d
),
daily_downtime AS (
    SELECT
        i.target_id,
        DATE(i.started_at) AS day,
        SUM(LEAST(
            COALESCE(i.duration_secs, EXTRACT(EPOCH FROM (NOW() - i.started_at))::INTEGER),
            86400
        )) AS downtime_secs
    FROM incidents i
    GROUP BY i.target_id, DATE(i.started_at)
)
SELECT
    ds.device_name,
    ds.target_type      AS device_type,
    ds.location,
    ds.department,
    ds.priority,
    ds.day              AS date,
    TO_CHAR(ds.day, 'YYYY-MM') AS month,
    COALESCE(dd.downtime_secs, 0)                               AS downtime_seconds,
    ROUND((86400 - COALESCE(dd.downtime_secs, 0)) / 864.0, 2)  AS uptime_pct
FROM day_series ds
LEFT JOIN daily_downtime dd
       ON dd.target_id = ds.target_id AND dd.day = ds.day;

-- ---------------------------------------------------------------------------
-- rpt_assets
-- Full asset inventory with book value and warranty status.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW reporting.rpt_assets AS
SELECT
    a.id                                        AS asset_id,
    a.asset_tag,
    a.serial_number,
    a.manufacturer,
    a.model,
    a.asset_type                                AS device_type,
    a.location,
    a.department,
    a.status                                    AS asset_status,
    t.name                                      AS linked_device,
    t.ip_address::text                          AS ip_address,
    a.purchase_date,
    a.purchase_price::float                     AS purchase_price_usd,
    a.useful_life_years,
    a.salvage_value::float                      AS salvage_value_usd,
    -- Straight-line book value
    CASE
        WHEN a.purchase_price IS NOT NULL AND a.purchase_date IS NOT NULL
        THEN GREATEST(
            COALESCE(a.salvage_value::float, 0),
            a.purchase_price::float - (
                (a.purchase_price::float - COALESCE(a.salvage_value::float, 0))
                / COALESCE(a.useful_life_years, 5)
                * (EXTRACT(EPOCH FROM (NOW() - a.purchase_date::TIMESTAMPTZ)) / 31557600)
            )
        )
    END                                         AS book_value_usd,
    -- End of life date
    CASE
        WHEN a.purchase_date IS NOT NULL AND a.useful_life_years IS NOT NULL
        THEN (a.purchase_date + (a.useful_life_years || ' years')::INTERVAL)::DATE
    END                                         AS end_of_life_date,
    -- Warranty
    w.warranty_type,
    w.vendor                                    AS warranty_vendor,
    w.end_date                                  AS warranty_end_date,
    CASE
        WHEN w.end_date IS NULL              THEN 'Unknown'
        WHEN w.end_date < CURRENT_DATE       THEN 'Expired'
        WHEN w.end_date < CURRENT_DATE + 90  THEN 'Expiring Soon'
        ELSE                                      'Active'
    END                                         AS warranty_status,
    (w.end_date - CURRENT_DATE)                 AS warranty_days_remaining
FROM assets a
LEFT JOIN targets t ON t.id = a.target_id
LEFT JOIN LATERAL (
    SELECT warranty_type, vendor, end_date
    FROM warranty_events
    WHERE asset_id = a.id AND event_type = 'registration'
    ORDER BY end_date DESC NULLS LAST
    LIMIT 1
) w ON TRUE
WHERE a.status != 'disposed';

-- ---------------------------------------------------------------------------
-- rpt_asset_tco
-- Total cost of ownership per asset.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW reporting.rpt_asset_tco AS
SELECT
    a.id                            AS asset_id,
    a.asset_tag,
    a.manufacturer,
    a.model,
    a.asset_type                    AS device_type,
    a.location,
    a.department,
    COALESCE(a.purchase_price::float, 0)    AS purchase_cost_usd,
    COALESCE(repairs.total_repair_cost, 0)  AS repair_cost_usd,
    COALESCE(repairs.repair_count, 0)       AS repair_count,
    COALESCE(inc.incident_count, 0)         AS incident_count,
    COALESCE(inc.total_downtime_hours, 0)   AS downtime_hours,
    ROUND((COALESCE(inc.total_downtime_hours, 0) * lp.rev_per_hour)::NUMERIC, 2)
                                            AS downtime_loss_usd,
    COALESCE(a.purchase_price::float, 0)
        + COALESCE(repairs.total_repair_cost, 0)
        + ROUND((COALESCE(inc.total_downtime_hours, 0) * lp.rev_per_hour)::NUMERIC, 2)
                                            AS total_cost_of_ownership_usd
FROM assets a
LEFT JOIN (
    SELECT asset_id,
           SUM(cost::float)  AS total_repair_cost,
           COUNT(*)           AS repair_count
    FROM asset_events
    WHERE event_type IN ('repair','replacement') AND cost IS NOT NULL
    GROUP BY asset_id
) repairs ON repairs.asset_id = a.id
LEFT JOIN (
    SELECT a2.id AS asset_id,
           COUNT(DISTINCT i.id)                          AS incident_count,
           SUM(COALESCE(i.duration_secs,0)) / 3600.0    AS total_downtime_hours
    FROM assets a2
    JOIN targets t   ON t.id = a2.target_id
    JOIN incidents i ON i.target_id = t.id
    WHERE i.resolved_at IS NOT NULL
    GROUP BY a2.id
) inc ON inc.asset_id = a.id
CROSS JOIN (
    SELECT COALESCE(param_value::float, 8500) AS rev_per_hour
    FROM loss_parameters
    WHERE param_scope = 'global' AND param_key = 'revenue_per_hour'
      AND effective_to IS NULL
    LIMIT 1
) lp
WHERE a.status != 'disposed';

-- ---------------------------------------------------------------------------
-- rpt_weather_incidents
-- Incidents joined with concurrent weather for Power BI weather correlation.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW reporting.rpt_weather_incidents AS
SELECT
    i.id                                        AS incident_id,
    t.name                                      AS device_name,
    t.target_type                               AS device_type,
    t.location,
    t.department,
    i.started_at,
    i.resolved_at,
    COALESCE(i.duration_secs, 0)                AS duration_seconds,
    ROUND(COALESCE(i.duration_secs,0) / 60.0,1) AS duration_minutes,
    DATE(i.started_at)                          AS incident_date,
    TO_CHAR(i.started_at, 'YYYY-MM')            AS incident_month,
    -- Weather alert (if any)
    wa.event_type                               AS weather_event,
    wa.severity                                 AS weather_severity,
    CASE WHEN wa.id IS NULL THEN 'No' ELSE 'Yes' END AS during_weather_alert,
    -- Nearest observation
    wo.condition                                AS weather_condition,
    wo.temp_f,
    wo.wind_speed_mph,
    wo.precip_1h_in
FROM incidents i
JOIN targets t ON t.id = i.target_id
LEFT JOIN incident_weather iw ON iw.incident_id = i.id
LEFT JOIN weather_alerts wa   ON wa.id = iw.alert_id
LEFT JOIN LATERAL (
    SELECT condition, temp_f, wind_speed_mph, precip_1h_in
    FROM weather_observations
    WHERE observed_at BETWEEN i.started_at - INTERVAL '30 minutes'
                          AND i.started_at + INTERVAL '30 minutes'
    ORDER BY ABS(EXTRACT(EPOCH FROM (observed_at - i.started_at)))
    LIMIT 1
) wo ON TRUE;

-- ---------------------------------------------------------------------------
-- rpt_device_health
-- Current status snapshot of all devices — live dashboard feed.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW reporting.rpt_device_health AS
SELECT
    t.id                                        AS target_id,
    t.name                                      AS device_name,
    t.target_type                               AS device_type,
    t.location,
    t.department,
    t.priority,
    CASE t.priority
        WHEN 1 THEN 'Critical'
        WHEN 2 THEN 'High'
        WHEN 3 THEN 'Normal'
        WHEN 4 THEN 'Low'
        ELSE 'Unknown'
    END                                         AS priority_label,
    t.ip_address::text                          AS ip_address,
    t.check_protocol,
    COALESCE(ts.status, 'unknown')              AS current_status,
    ts.last_check_at,
    ts.last_success_at,
    ts.last_failure_at,
    COALESCE(ts.consecutive_failures, 0)        AS consecutive_failures,
    ts.current_latency_ms,
    -- Open incident
    oi.id                                       AS open_incident_id,
    oi.started_at                               AS outage_started_at,
    ROUND(EXTRACT(EPOCH FROM (NOW() - oi.started_at)) / 60.0, 1)
                                                AS current_outage_minutes,
    -- 30-day stats
    stats.incident_count_30d,
    ROUND(stats.total_downtime_mins_30d, 1)     AS downtime_minutes_30d,
    ROUND(stats.uptime_pct_30d, 2)              AS uptime_pct_30d
FROM targets t
LEFT JOIN target_state ts ON ts.target_id = t.id
LEFT JOIN LATERAL (
    SELECT id, started_at FROM incidents
    WHERE target_id = t.id AND resolved_at IS NULL
    ORDER BY started_at DESC LIMIT 1
) oi ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*)                                            AS incident_count_30d,
        SUM(COALESCE(duration_secs,0)) / 60.0              AS total_downtime_mins_30d,
        100.0 - (SUM(COALESCE(duration_secs,0)) / 25920.0) AS uptime_pct_30d
    FROM incidents
    WHERE target_id = t.id
      AND started_at >= NOW() - INTERVAL '30 days'
      AND resolved_at IS NOT NULL
) stats ON TRUE
WHERE t.enabled = TRUE;
