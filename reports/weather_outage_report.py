"""
Weather & Outage Actuarial Report
Pine Mountain, GA — Resort IT

Calculates:
  1. Outage frequency by weather event type
  2. Average downtime duration per weather event type
  3. Estimated annual financial loss from weather-related outages
  4. Forecast: expected outages and losses for the next 12 months

Run:
    python3 -m reports.weather_outage_report
    python3 -m reports.weather_outage_report --json
"""

import argparse
import json
import math
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from collections import defaultdict

from collector.config import DB_DSN


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch(cur, sql, params=None):
    cur.execute(sql, params or ())
    return cur.fetchall()


def poisson_forecast(rate_per_year, periods=12):
    """
    Given an average annual event rate (lambda), return:
      - expected events over `periods` months
      - 80% confidence interval using normal approximation to Poisson
    """
    expected = rate_per_year * (periods / 12)
    # Poisson std dev = sqrt(lambda)
    std_dev  = math.sqrt(expected) if expected > 0 else 0
    low      = max(0, round(expected - 1.28 * std_dev, 1))   # 10th percentile
    high     = round(expected + 1.28 * std_dev, 1)            # 90th percentile
    return expected, low, high


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def get_observation_window(cur):
    """How many days of data do we have?"""
    cur.execute("""
        SELECT
            MIN(started_at) AS first_incident,
            MAX(started_at) AS last_incident,
            COUNT(*)        AS total_incidents
        FROM incidents
    """)
    row = cur.fetchone()
    if row["first_incident"] is None:
        return 0, row
    delta = (row["last_incident"] - row["first_incident"]).days or 1
    return delta, row


def get_weather_outage_stats(cur):
    """Incidents per weather event type with duration and loss data."""
    cur.execute("""
        SELECT
            wa.event_type,
            wa.severity,
            COUNT(DISTINCT i.id)                            AS incident_count,
            COUNT(DISTINCT i.target_id)                     AS targets_affected,
            AVG(COALESCE(i.duration_secs, 0))               AS avg_duration_secs,
            SUM(COALESCE(i.duration_secs, 0))               AS total_duration_secs,
            MAX(COALESCE(i.duration_secs, 0))               AS max_duration_secs
        FROM incident_weather iw
        JOIN weather_alerts wa ON wa.id  = iw.alert_id
        JOIN incidents i       ON i.id   = iw.incident_id
        GROUP BY wa.event_type, wa.severity
        ORDER BY incident_count DESC
    """)
    return cur.fetchall()


def get_non_weather_outages(cur):
    """Incidents with no weather correlation."""
    cur.execute("""
        SELECT
            COUNT(*)                        AS incident_count,
            AVG(COALESCE(duration_secs, 0)) AS avg_duration_secs,
            SUM(COALESCE(duration_secs, 0)) AS total_duration_secs
        FROM incidents
        WHERE id NOT IN (SELECT incident_id FROM incident_weather)
    """)
    return cur.fetchone()


def get_loss_rate(cur):
    """Revenue per hour from loss_parameters."""
    cur.execute("""
        SELECT param_value FROM loss_parameters
        WHERE param_scope = 'global'
          AND param_key   = 'revenue_per_hour'
          AND effective_to IS NULL
        LIMIT 1
    """)
    row = cur.fetchone()
    return float(row["param_value"]) if row else 8500.0


def get_alert_frequency(cur, days_observed):
    """How often does each alert type occur per year?"""
    cur.execute("""
        SELECT event_type, COUNT(*) AS occurrences
        FROM weather_alerts
        GROUP BY event_type
        ORDER BY occurrences DESC
    """)
    rows = cur.fetchall()
    years = max(days_observed / 365.25, 1/365.25)
    return [
        {
            "event_type":    r["event_type"],
            "occurrences":   r["occurrences"],
            "rate_per_year": round(r["occurrences"] / years, 2),
        }
        for r in rows
    ]


def get_outage_by_condition(cur):
    """Outages correlated with raw observation conditions (thunderstorm, rain, etc.)."""
    cur.execute("""
        SELECT
            CASE
                WHEN LOWER(wo.condition) LIKE '%thunder%'  THEN 'Thunderstorm'
                WHEN LOWER(wo.condition) LIKE '%tornado%'  THEN 'Tornado'
                WHEN LOWER(wo.condition) LIKE '%hurricane%'THEN 'Hurricane'
                WHEN LOWER(wo.condition) LIKE '%rain%'     THEN 'Rain'
                WHEN LOWER(wo.condition) LIKE '%snow%'     THEN 'Snow/Ice'
                WHEN LOWER(wo.condition) LIKE '%ice%'      THEN 'Snow/Ice'
                WHEN LOWER(wo.condition) LIKE '%fog%'      THEN 'Fog'
                WHEN LOWER(wo.condition) LIKE '%wind%'     THEN 'High Wind'
                ELSE 'Other'
            END                             AS condition_group,
            COUNT(DISTINCT i.id)            AS incident_count,
            AVG(COALESCE(i.duration_secs,0))AS avg_duration_secs
        FROM incidents i
        JOIN weather_observations wo
          ON wo.observed_at BETWEEN i.started_at - INTERVAL '30 minutes'
                                AND i.started_at + INTERVAL '30 minutes'
        GROUP BY condition_group
        ORDER BY incident_count DESC
    """)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Report renderer
# ---------------------------------------------------------------------------

def render_text(data):
    d               = data
    rev             = d["revenue_per_hour"]
    days            = d["days_observed"]
    years           = max(days / 365.25, 0.001)

    lines = []
    lines.append("=" * 70)
    lines.append("  WEATHER & OUTAGE ACTUARIAL REPORT")
    lines.append(f"  Pine Mountain, GA  —  Resort IT  —  {datetime.now():%Y-%m-%d %H:%M}")
    lines.append("=" * 70)
    lines.append(f"\n  Observation window : {days} days  ({years:.2f} years)")
    lines.append(f"  Total incidents    : {d['total_incidents']}")
    lines.append(f"  Revenue/hour used  : ${rev:,.2f}")

    # --- Weather-correlated outages
    lines.append("\n" + "-" * 70)
    lines.append("  OUTAGES BY WEATHER EVENT TYPE")
    lines.append("-" * 70)

    if d["weather_stats"]:
        lines.append(f"  {'Event Type':<35} {'Count':>6} {'Avg Min':>8} {'Loss Est':>12} {'Annual Rate':>12}")
        lines.append(f"  {'-'*35} {'-'*6} {'-'*8} {'-'*12} {'-'*12}")
        for s in d["weather_stats"]:
            avg_hrs   = s["avg_duration_secs"] / 3600
            loss_est  = avg_hrs * rev * s["incident_count"]
            ann_rate  = s["incident_count"] / years
            lines.append(
                f"  {s['event_type']:<35} {s['incident_count']:>6} "
                f"{s['avg_duration_secs']/60:>7.1f}m "
                f"${loss_est:>11,.0f} "
                f"{ann_rate:>10.1f}/yr"
            )
    else:
        lines.append("  No weather-correlated outages yet. Run the correlator after incidents accumulate.")

    # --- Non-weather outages
    lines.append("\n" + "-" * 70)
    lines.append("  NON-WEATHER OUTAGES")
    lines.append("-" * 70)
    nw = d["non_weather"]
    if nw and nw["incident_count"]:
        nw_hrs  = (nw["total_duration_secs"] or 0) / 3600
        nw_loss = nw_hrs * rev
        lines.append(f"  Count          : {nw['incident_count']}")
        lines.append(f"  Avg duration   : {(nw['avg_duration_secs'] or 0)/60:.1f} min")
        lines.append(f"  Estimated loss : ${nw_loss:,.0f}")
    else:
        lines.append("  None recorded.")

    # --- Outage by weather condition
    lines.append("\n" + "-" * 70)
    lines.append("  OUTAGES BY OBSERVED CONDITION (±30 min window)")
    lines.append("-" * 70)
    if d["condition_stats"]:
        lines.append(f"  {'Condition':<20} {'Incidents':>10} {'Avg Duration':>14}")
        lines.append(f"  {'-'*20} {'-'*10} {'-'*14}")
        for c in d["condition_stats"]:
            lines.append(
                f"  {c['condition_group']:<20} {c['incident_count']:>10} "
                f"{c['avg_duration_secs']/60:>12.1f}m"
            )
    else:
        lines.append("  Insufficient overlapping observation/incident data.")

    # --- Alert frequency
    lines.append("\n" + "-" * 70)
    lines.append("  HISTORICAL ALERT FREQUENCY (from NWS data)")
    lines.append("-" * 70)
    if d["alert_frequency"]:
        lines.append(f"  {'Alert Type':<40} {'Total':>6} {'Per Year':>10}")
        lines.append(f"  {'-'*40} {'-'*6} {'-'*10}")
        for a in d["alert_frequency"]:
            lines.append(
                f"  {a['event_type']:<40} {a['occurrences']:>6} {a['rate_per_year']:>9.1f}"
            )
    else:
        lines.append("  No alert history yet.")

    # --- 12-month forecast
    lines.append("\n" + "-" * 70)
    lines.append("  12-MONTH ACTUARIAL FORECAST  (Poisson model, 80% CI)")
    lines.append("-" * 70)

    if d["weather_stats"]:
        total_weather_incidents = sum(s["incident_count"] for s in d["weather_stats"])
        total_weather_loss_hrs  = sum(
            s["total_duration_secs"] / 3600 for s in d["weather_stats"]
        )
        ann_inc_rate = total_weather_incidents / years
        ann_loss_hrs = total_weather_loss_hrs / years

        exp_inc, lo_inc, hi_inc = poisson_forecast(ann_inc_rate, 12)
        exp_hrs, lo_hrs, hi_hrs = poisson_forecast(ann_loss_hrs, 12)

        lines.append(f"\n  Weather-related incidents (next 12 months):")
        lines.append(f"    Expected  : {exp_inc:.1f}  (80% CI: {lo_inc}–{hi_inc})")
        lines.append(f"\n  Downtime hours (next 12 months):")
        lines.append(f"    Expected  : {exp_hrs:.1f} hrs  (80% CI: {lo_hrs}–{hi_hrs} hrs)")
        lines.append(f"\n  Estimated revenue at risk (next 12 months):")
        lines.append(f"    Expected  : ${exp_hrs * rev:,.0f}")
        lines.append(f"    Low       : ${lo_hrs  * rev:,.0f}")
        lines.append(f"    High      : ${hi_hrs  * rev:,.0f}")

        # Per event-type breakdown
        lines.append(f"\n  By event type:")
        lines.append(f"  {'Event Type':<35} {'Exp Incidents':>14} {'Exp Loss':>12}")
        lines.append(f"  {'-'*35} {'-'*14} {'-'*12}")
        for s in d["weather_stats"]:
            rate   = s["incident_count"] / years
            exp, lo, hi = poisson_forecast(rate, 12)
            avg_loss_per_inc = (s["avg_duration_secs"] / 3600) * rev
            lines.append(
                f"  {s['event_type']:<35} {exp:>10.1f} ({lo}–{hi})"
                f"  ${exp * avg_loss_per_inc:>10,.0f}"
            )
    else:
        lines.append("  Insufficient data for forecast. Accumulate incidents over time.")

    lines.append("\n" + "=" * 70)
    lines.append("  END OF REPORT")
    lines.append("=" * 70)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_report():
    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            days, window = get_observation_window(cur)
            data = {
                "generated_at":      datetime.now(timezone.utc).isoformat(),
                "days_observed":     days,
                "total_incidents":   window["total_incidents"] or 0,
                "revenue_per_hour":  get_loss_rate(cur),
                "weather_stats":     [dict(r) for r in get_weather_outage_stats(cur)],
                "non_weather":       dict(get_non_weather_outages(cur)),
                "condition_stats":   [dict(r) for r in get_outage_by_condition(cur)],
                "alert_frequency":   get_alert_frequency(cur, days),
            }
    return data


def main():
    parser = argparse.ArgumentParser(description="Weather outage actuarial report")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--out", help="Write report to file instead of stdout")
    args = parser.parse_args()

    data = build_report()

    if args.json:
        output = json.dumps(data, indent=2, default=str)
    else:
        output = render_text(data)

    if args.out:
        with open(args.out, "w") as f:
            f.write(output)
        print(f"Report written to {args.out}")
    else:
        print(output)


if __name__ == "__main__":
    main()
