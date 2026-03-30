"""
Asset Management Report

Covers:
  1. Full asset inventory with current book value (straight-line depreciation)
  2. Warranty status — active, expiring soon (<90 days), expired
  3. Total Cost of Ownership (purchase + repairs + downtime loss)
  4. Replacement forecast — assets projected to reach end of life within 12 months

Run:
    python3 -m reports.asset_report
    python3 -m reports.asset_report --json
    python3 -m reports.asset_report --out reports/output/assets.txt
"""

import argparse
import json
from datetime import date, datetime, timezone
from decimal import Decimal

import psycopg2
import psycopg2.extras

from collector.config import DB_DSN


# ---------------------------------------------------------------------------
# Depreciation
# ---------------------------------------------------------------------------

def straight_line_book_value(purchase_price, salvage_value, useful_life_years,
                              purchase_date):
    """Current book value using straight-line depreciation."""
    if not purchase_price or not purchase_date:
        return None
    try:
        purchase = date.fromisoformat(str(purchase_date))
    except (ValueError, TypeError):
        return None

    years_held  = (date.today() - purchase).days / 365.25
    life        = useful_life_years or 5
    salvage     = float(salvage_value or 0)
    cost        = float(purchase_price)
    annual_dep  = (cost - salvage) / life
    book_value  = max(salvage, cost - (annual_dep * years_held))
    return round(book_value, 2)


def years_remaining(purchase_date, useful_life_years):
    if not purchase_date or not useful_life_years:
        return None
    try:
        purchase = date.fromisoformat(str(purchase_date))
    except (ValueError, TypeError):
        return None
    end_of_life = date(purchase.year + useful_life_years,
                       purchase.month, purchase.day)
    return round((end_of_life - date.today()).days / 365.25, 1)


# ---------------------------------------------------------------------------
# Data queries
# ---------------------------------------------------------------------------

def get_assets(cur):
    cur.execute("""
        SELECT
            a.id, a.asset_tag, a.serial_number, a.manufacturer, a.model,
            a.asset_type, a.location, a.department, a.status,
            a.purchase_date, a.purchase_price::float, a.salvage_value::float,
            a.useful_life_years, a.notes,
            t.name AS target_name,
            t.ip_address::text AS ip_address
        FROM assets a
        LEFT JOIN targets t ON t.id = a.target_id
        WHERE a.status != 'disposed'
        ORDER BY a.asset_type, a.manufacturer, a.model
    """)
    return cur.fetchall()


def get_warranty_status(cur):
    cur.execute("""
        SELECT
            a.id AS asset_id,
            a.asset_tag,
            a.manufacturer,
            a.model,
            a.location,
            a.department,
            w.warranty_type,
            w.vendor,
            w.end_date,
            CASE
                WHEN w.end_date IS NULL              THEN 'unknown'
                WHEN w.end_date < CURRENT_DATE       THEN 'expired'
                WHEN w.end_date < CURRENT_DATE + 90  THEN 'expiring_soon'
                ELSE                                      'active'
            END AS warranty_status,
            (w.end_date - CURRENT_DATE) AS days_remaining
        FROM assets a
        LEFT JOIN LATERAL (
            SELECT warranty_type, vendor, end_date
            FROM warranty_events
            WHERE asset_id = a.id AND event_type = 'registration'
            ORDER BY end_date DESC NULLS LAST
            LIMIT 1
        ) w ON TRUE
        WHERE a.status = 'active'
        ORDER BY w.end_date ASC NULLS LAST
    """)
    return cur.fetchall()


def get_repair_costs(cur):
    cur.execute("""
        SELECT asset_id, SUM(cost::float) AS total_repair_cost, COUNT(*) AS repair_count
        FROM asset_events
        WHERE event_type IN ('repair', 'replacement')
          AND cost IS NOT NULL
        GROUP BY asset_id
    """)
    return {row["asset_id"]: row for row in cur.fetchall()}


def get_downtime_loss(cur):
    """Estimated revenue loss per asset via linked incidents."""
    cur.execute("""
        SELECT
            a.id AS asset_id,
            COUNT(DISTINCT i.id)                         AS incident_count,
            SUM(COALESCE(i.duration_secs, 0))            AS total_downtime_secs
        FROM assets a
        JOIN targets t   ON t.id  = a.target_id
        JOIN incidents i ON i.target_id = t.id
        WHERE i.resolved_at IS NOT NULL
        GROUP BY a.id
    """)
    return {row["asset_id"]: row for row in cur.fetchall()}


def get_revenue_per_hour(cur):
    cur.execute("""
        SELECT param_value::float FROM loss_parameters
        WHERE param_scope = 'global' AND param_key = 'revenue_per_hour'
          AND effective_to IS NULL
        LIMIT 1
    """)
    row = cur.fetchone()
    return row["param_value"] if row else 8500.0


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

def build_report():
    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            assets          = get_assets(cur)
            warranty_rows   = get_warranty_status(cur)
            repair_costs    = get_repair_costs(cur)
            downtime_losses = get_downtime_loss(cur)
            rev_per_hour    = get_revenue_per_hour(cur)

    # Enrich assets
    enriched = []
    for a in assets:
        aid         = a["id"]
        book_value  = straight_line_book_value(
            a["purchase_price"], a["salvage_value"],
            a["useful_life_years"], a["purchase_date"]
        )
        yrs_left    = years_remaining(a["purchase_date"], a["useful_life_years"])
        repairs     = repair_costs.get(aid, {})
        downtime    = downtime_losses.get(aid, {})
        dt_loss     = ((downtime.get("total_downtime_secs") or 0) / 3600) * rev_per_hour

        tco = sum(filter(None, [
            a["purchase_price"],
            repairs.get("total_repair_cost"),
            dt_loss,
        ]))

        enriched.append({
            **dict(a),
            "book_value":         book_value,
            "years_remaining":    yrs_left,
            "repair_cost":        repairs.get("total_repair_cost", 0),
            "repair_count":       repairs.get("repair_count", 0),
            "incident_count":     downtime.get("incident_count", 0),
            "downtime_loss":      round(dt_loss, 2),
            "total_cost_of_ownership": round(tco, 2),
        })

    return {
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "assets":         enriched,
        "warranty":       [dict(r) for r in warranty_rows],
        "revenue_per_hour": rev_per_hour,
    }


# ---------------------------------------------------------------------------
# Text renderer
# ---------------------------------------------------------------------------

def render_text(data):
    lines = []
    lines.append("=" * 72)
    lines.append("  ASSET MANAGEMENT REPORT")
    lines.append(f"  Resort IT  —  {datetime.now():%Y-%m-%d %H:%M}")
    lines.append("=" * 72)

    assets = data["assets"]
    lines.append(f"\n  Total assets tracked: {len(assets)}")

    # --- Inventory by type
    lines.append("\n" + "-" * 72)
    lines.append("  FULL INVENTORY")
    lines.append("-" * 72)
    lines.append(f"  {'Asset Tag':<12} {'Manufacturer':<14} {'Model':<20} "
                 f"{'Type':<12} {'Location':<16} {'Book Value':>10} {'EOL':>6}")
    lines.append(f"  {'-'*12} {'-'*14} {'-'*20} {'-'*12} {'-'*16} {'-'*10} {'-'*6}")

    for a in assets:
        bv  = f"${a['book_value']:,.0f}" if a["book_value"] is not None else "N/A"
        eol = f"{a['years_remaining']:.1f}yr" if a["years_remaining"] is not None else "N/A"
        lines.append(
            f"  {(a['asset_tag'] or '-'):<12} "
            f"{(a['manufacturer'] or '-'):<14} "
            f"{(a['model'] or '-'):<20} "
            f"{a['asset_type']:<12} "
            f"{(a['location'] or '-'):<16} "
            f"{bv:>10} "
            f"{eol:>6}"
        )

    # --- Warranty status
    lines.append("\n" + "-" * 72)
    lines.append("  WARRANTY STATUS")
    lines.append("-" * 72)

    expired      = [w for w in data["warranty"] if w["warranty_status"] == "expired"]
    expiring     = [w for w in data["warranty"] if w["warranty_status"] == "expiring_soon"]
    active       = [w for w in data["warranty"] if w["warranty_status"] == "active"]
    unknown      = [w for w in data["warranty"] if w["warranty_status"] == "unknown"]

    def warranty_block(title, rows, show_days=True):
        if not rows:
            return
        lines.append(f"\n  {title} ({len(rows)})")
        lines.append(f"  {'Asset Tag':<12} {'Manufacturer':<14} {'Model':<20} "
                     f"{'Vendor':<18} {'Expires':<12} {'Days':>6}")
        lines.append(f"  {'-'*12} {'-'*14} {'-'*20} {'-'*18} {'-'*12} {'-'*6}")
        for w in rows:
            exp   = str(w["end_date"]) if w["end_date"] else "Unknown"
            days  = str(w["days_remaining"]) if w["days_remaining"] is not None else "N/A"
            lines.append(
                f"  {(w['asset_tag'] or '-'):<12} "
                f"{(w['manufacturer'] or '-'):<14} "
                f"{(w['model'] or '-'):<20} "
                f"{(w['vendor'] or '-'):<18} "
                f"{exp:<12} "
                f"{days:>6}"
            )

    warranty_block("!! EXPIRED", expired)
    warranty_block("!! EXPIRING WITHIN 90 DAYS", expiring)
    warranty_block("Active", active)
    if unknown:
        lines.append(f"\n  No warranty info: {len(unknown)} asset(s)")

    # --- TCO
    lines.append("\n" + "-" * 72)
    lines.append("  TOTAL COST OF OWNERSHIP")
    lines.append("-" * 72)
    lines.append(f"  {'Asset Tag':<12} {'Model':<20} {'Purchase':>10} "
                 f"{'Repairs':>9} {'Downtime $':>11} {'TCO':>11} {'Incidents':>10}")
    lines.append(f"  {'-'*12} {'-'*20} {'-'*10} {'-'*9} {'-'*11} {'-'*11} {'-'*10}")

    total_tco = 0
    for a in sorted(assets, key=lambda x: x["total_cost_of_ownership"], reverse=True):
        pp  = f"${a['purchase_price']:,.0f}"  if a["purchase_price"]  else "N/A"
        rc  = f"${a['repair_cost']:,.0f}"     if a["repair_cost"]     else "$0"
        dl  = f"${a['downtime_loss']:,.0f}"
        tco = f"${a['total_cost_of_ownership']:,.0f}"
        total_tco += a["total_cost_of_ownership"] or 0
        lines.append(
            f"  {(a['asset_tag'] or '-'):<12} "
            f"{(a['model'] or '-'):<20} "
            f"{pp:>10} "
            f"{rc:>9} "
            f"{dl:>11} "
            f"{tco:>11} "
            f"{a['incident_count']:>10}"
        )
    lines.append(f"\n  Total TCO (all assets): ${total_tco:,.0f}")

    # --- Replacement forecast
    lines.append("\n" + "-" * 72)
    lines.append("  REPLACEMENT FORECAST — End of Life within 24 months")
    lines.append("-" * 72)
    eol_soon = [a for a in assets
                if a["years_remaining"] is not None and a["years_remaining"] <= 2]

    if eol_soon:
        eol_soon.sort(key=lambda x: x["years_remaining"])
        lines.append(f"  {'Asset Tag':<12} {'Manufacturer':<14} {'Model':<20} "
                     f"{'Type':<12} {'EOL In':>8} {'Book Val':>10} {'Est. Replace':>13}")
        lines.append(f"  {'-'*12} {'-'*14} {'-'*20} {'-'*12} {'-'*8} {'-'*10} {'-'*13}")
        total_replace = 0
        for a in eol_soon:
            bv  = a["book_value"] or 0
            pp  = a["purchase_price"] or 0
            est = pp * 1.10   # estimate replacement at 110% of original cost
            total_replace += est
            lines.append(
                f"  {(a['asset_tag'] or '-'):<12} "
                f"{(a['manufacturer'] or '-'):<14} "
                f"{(a['model'] or '-'):<20} "
                f"{a['asset_type']:<12} "
                f"{a['years_remaining']:>6.1f}yr "
                f"${bv:>9,.0f} "
                f"${est:>12,.0f}"
            )
        lines.append(f"\n  Estimated replacement budget needed: ${total_replace:,.0f}")
    else:
        lines.append("  No assets approaching end of life within 24 months.")

    lines.append("\n" + "=" * 72)
    lines.append("  END OF REPORT")
    lines.append("=" * 72)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Asset management report")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--out", help="Write to file instead of stdout")
    args = parser.parse_args()

    data = build_report()

    if args.json:
        output = json.dumps(data, indent=2, default=str)
    else:
        output = render_text(data)

    if args.out:
        import os
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, "w") as f:
            f.write(output)
        print(f"Report written to {args.out}")
    else:
        print(output)


if __name__ == "__main__":
    main()
