"""
CSV Exporter for Power BI

Exports all reporting schema views to CSV files that Power BI can
import directly via Get Data → Text/CSV.

Run:
    python3 -m reports.export_csv
    python3 -m reports.export_csv --out-dir /path/to/output
"""

import argparse
import csv
import os
from datetime import datetime

import psycopg2
import psycopg2.extras

from collector.config import DB_DSN

DEFAULT_OUT_DIR = "reports/output/powerbi"

EXPORTS = [
    {
        "view":     "reporting.rpt_device_health",
        "filename": "device_health.csv",
        "desc":     "Current device status — live dashboard",
    },
    {
        "view":     "reporting.rpt_incidents",
        "filename": "incidents.csv",
        "desc":     "All incidents with date/time/duration breakdown",
    },
    {
        "view":     "reporting.rpt_uptime_summary",
        "filename": "uptime_summary.csv",
        "desc":     "Daily uptime % per device (last 90 days)",
    },
    {
        "view":     "reporting.rpt_assets",
        "filename": "assets.csv",
        "desc":     "Asset inventory with book value and warranty status",
    },
    {
        "view":     "reporting.rpt_asset_tco",
        "filename": "asset_tco.csv",
        "desc":     "Total cost of ownership per asset",
    },
    {
        "view":     "reporting.rpt_weather_incidents",
        "filename": "weather_incidents.csv",
        "desc":     "Incidents correlated with weather conditions",
    },
]


def export_view(cur, view, filepath):
    cur.execute(f"SELECT * FROM {view}")
    rows = cur.fetchall()

    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        else:
            f.write("")   # empty file, Power BI handles it gracefully

    return len(rows)


def run(out_dir=DEFAULT_OUT_DIR):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    export_dir = os.path.join(out_dir, timestamp)

    print(f"\nExporting Power BI data to: {export_dir}\n")

    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            for exp in EXPORTS:
                filepath = os.path.join(export_dir, exp["filename"])
                try:
                    count = export_view(cur, exp["view"], filepath)
                    print(f"  {exp['filename']:<30} {count:>6} rows  — {exp['desc']}")
                except Exception as e:
                    print(f"  {exp['filename']:<30} ERROR: {e}")

    # Write a manifest
    manifest_path = os.path.join(export_dir, "manifest.txt")
    with open(manifest_path, "w") as f:
        f.write(f"Export generated: {datetime.now():%Y-%m-%d %H:%M}\n")
        f.write(f"Database: downtime_monitor\n")
        f.write(f"Location: Pine Mountain, GA\n\n")
        f.write("Files:\n")
        for exp in EXPORTS:
            f.write(f"  {exp['filename']} — {exp['desc']}\n")
        f.write("\nTo use in Power BI:\n")
        f.write("  Option A: Home → Get Data → Text/CSV → select each file\n")
        f.write("  Option B: Connect Power BI directly to PostgreSQL:\n")
        f.write("            Home → Get Data → PostgreSQL\n")
        f.write("            Server: localhost   Database: downtime_monitor\n")
        f.write("            Use schema: reporting\n")

    print(f"\n  Manifest written to {manifest_path}")
    print(f"\nDone. To load in Power BI:")
    print(f"  Home → Get Data → Text/CSV → select files from:")
    print(f"  {export_dir}\n")
    print(f"  OR connect directly:")
    print(f"  Home → Get Data → PostgreSQL")
    print(f"  Server: localhost   Database: downtime_monitor   Schema: reporting\n")


def main():
    parser = argparse.ArgumentParser(description="Export reporting views to CSV for Power BI")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR,
                        help=f"Output directory (default: {DEFAULT_OUT_DIR})")
    args = parser.parse_args()
    run(out_dir=args.out_dir)


if __name__ == "__main__":
    main()
