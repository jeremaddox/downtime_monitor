"""
Interactive asset registration tool.

Run:
    python3 scripts/add_asset.py
"""

import sys
import psycopg2
import psycopg2.extras
from datetime import date

DB_DSN = "dbname=downtime_monitor"

ASSET_TYPES = [
    "server",
    "workstation",
    "switch",
    "ap",
    "camera",
    "pos",
    "kds",
    "printer",
    "tablet",
    "ups",
    "phone",
    "other",
]

DEPARTMENTS = [
    "IT",
    "F&B",
    "Retail",
    "Golf",
    "Housekeeping",
    "Front Desk",
    "Security",
    "Maintenance",
    "Management",
    "Other",
]

WARRANTY_TYPES = [
    "manufacturer",
    "extended",
    "on-site",
    "depot",
    "none",
]


# ---------------------------------------------------------------------------
# Input helpers
# ---------------------------------------------------------------------------

def prompt(label, default=None, required=True):
    suffix = f" [{default}]" if default is not None else ""
    while True:
        val = input(f"  {label}{suffix}: ").strip()
        if not val and default is not None:
            return str(default)
        if not val and required:
            print("    ! This field is required.")
            continue
        return val or None


def prompt_choice(label, options: dict, default_key=None):
    print(f"\n  {label}")
    for key, item in options.items():
        desc = item[1] if isinstance(item, tuple) else item
        marker = " (default)" if key == default_key else ""
        print(f"    {key}) {desc}{marker}")
    while True:
        choice = input("  Choose: ").strip() or default_key
        if choice in options:
            val = options[choice]
            return val[0] if isinstance(val, tuple) else val
        print(f"    ! Enter one of: {', '.join(options.keys())}")


def prompt_list(label, items):
    print(f"\n  {label}")
    indexed = {str(i + 1): v for i, v in enumerate(items)}
    for k, v in indexed.items():
        print(f"    {k}) {v}")
    print(f"    {len(items)+1}) Other (type your own)")
    while True:
        choice = input("  Choose: ").strip()
        if choice in indexed:
            return indexed[choice]
        if choice == str(len(items) + 1):
            return prompt("Enter value", required=True)
        print(f"    ! Enter a number between 1 and {len(items)+1}")


def prompt_decimal(label, default=None, required=False):
    while True:
        raw = prompt(label, default=default, required=required)
        if raw is None:
            return None
        try:
            return float(raw.replace(",", "").replace("$", ""))
        except ValueError:
            print("    ! Enter a number (e.g. 1250.00)")


def prompt_date(label, default=None, required=False):
    suffix = f" [{default}]" if default else " (YYYY-MM-DD, optional)"
    while True:
        raw = input(f"  {label}{suffix}: ").strip()
        if not raw:
            if default:
                return default
            if not required:
                return None
            print("    ! This field is required.")
            continue
        try:
            date.fromisoformat(raw)
            return raw
        except ValueError:
            print("    ! Use format YYYY-MM-DD (e.g. 2024-03-15)")


def prompt_int(label, default=None, required=False, min_val=None):
    while True:
        raw = prompt(label, default=default, required=required)
        if raw is None:
            return None
        try:
            val = int(raw)
            if min_val is not None and val < min_val:
                print(f"    ! Minimum value is {min_val}")
                continue
            return val
        except ValueError:
            print("    ! Enter a whole number.")


def confirm(sections):
    print("\n" + "=" * 55)
    print("  Review before saving")
    print("=" * 55)
    for section, fields in sections.items():
        print(f"\n  {section}")
        for k, v in fields.items():
            if v is not None:
                print(f"    {k:<22} {v}")
    print("\n" + "=" * 55)
    answer = input("\n  Save this asset? (yes/no) [yes]: ").strip().lower() or "yes"
    return answer in ("yes", "y")


# ---------------------------------------------------------------------------
# Link to existing target
# ---------------------------------------------------------------------------

def pick_target(cur):
    cur.execute("""
        SELECT id, name, ip_address::text, target_type
        FROM targets ORDER BY name
    """)
    targets = cur.fetchall()
    if not targets:
        return None

    print("\n  Link to a monitored target? (optional)")
    print("    0) Skip — not linked to a monitored device")
    for i, t in enumerate(targets, 1):
        print(f"    {i}) {t['name']}  ({t['ip_address']}  {t['target_type']})")

    while True:
        choice = input("  Choose: ").strip()
        if choice == "0" or choice == "":
            return None
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(targets):
                return targets[idx]["id"]
        except ValueError:
            pass
        print(f"    ! Enter 0 to skip or a number between 1 and {len(targets)}")


# ---------------------------------------------------------------------------
# Save asset + warranty
# ---------------------------------------------------------------------------

def save_asset(asset, warranty):
    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # Check duplicate asset tag
            if asset.get("asset_tag"):
                cur.execute("SELECT id FROM assets WHERE asset_tag = %s",
                            (asset["asset_tag"],))
                if cur.fetchone():
                    print(f"\n  ! Asset tag '{asset['asset_tag']}' already exists.")
                    return None

            cur.execute("""
                INSERT INTO assets
                    (target_id, asset_tag, serial_number, manufacturer, model,
                     asset_type, location, department, purchase_date,
                     purchase_price, currency, useful_life_years,
                     salvage_value, notes)
                VALUES
                    (%(target_id)s, %(asset_tag)s, %(serial_number)s,
                     %(manufacturer)s, %(model)s, %(asset_type)s,
                     %(location)s, %(department)s, %(purchase_date)s,
                     %(purchase_price)s, 'USD', %(useful_life_years)s,
                     %(salvage_value)s, %(notes)s)
                RETURNING id
            """, asset)
            asset_id = cur.fetchone()["id"]

            if warranty and warranty.get("warranty_type") != "none":
                cur.execute("""
                    INSERT INTO warranty_events
                        (asset_id, event_type, warranty_type, vendor,
                         contract_number, start_date, end_date)
                    VALUES
                        (%s, 'registration', %s, %s, %s, %s, %s)
                """, (
                    asset_id,
                    warranty["warranty_type"],
                    warranty.get("vendor"),
                    warranty.get("contract_number"),
                    warranty.get("start_date"),
                    warranty.get("end_date"),
                ))

            return asset_id


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

def run():
    print("\n" + "=" * 55)
    print("  Add Asset")
    print("=" * 55)

    asset   = {}
    warranty = {}

    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            asset["target_id"] = pick_target(cur)

    # Device info
    print("\n  -- Device Info --")
    asset["asset_tag"]     = prompt("Asset tag (e.g. AST-0042)", required=False)
    asset["serial_number"] = prompt("Serial number", required=False)
    asset["manufacturer"]  = prompt("Manufacturer (e.g. Cisco, Dell, HP)")
    asset["model"]         = prompt("Model (e.g. Catalyst 9300, OptiPlex 7090)")
    asset["asset_type"]    = prompt_list("Asset type", ASSET_TYPES)
    asset["location"]      = prompt("Physical location (e.g. Server Room, Kitchen)", default="Resort")
    asset["department"]    = prompt_list("Department", DEPARTMENTS)
    asset["notes"]         = prompt("Notes (optional)", required=False)

    # Financial info
    print("\n  -- Purchase & Financial Info --")
    asset["purchase_date"]      = prompt_date("Purchase date")
    asset["purchase_price"]     = prompt_decimal("Purchase price ($)", required=False)
    asset["useful_life_years"]  = prompt_int("Useful life (years, for depreciation)", default=5)
    asset["salvage_value"]      = prompt_decimal("Salvage value ($) at end of life", default="0")

    # Warranty
    print("\n  -- Warranty --")
    warranty["warranty_type"] = prompt_list("Warranty type", WARRANTY_TYPES)

    if warranty["warranty_type"] != "none":
        warranty["vendor"]          = prompt("Warranty vendor (e.g. Dell ProSupport)", required=False)
        warranty["contract_number"] = prompt("Contract/agreement number", required=False)
        warranty["start_date"]      = prompt_date("Warranty start date")
        warranty["end_date"]        = prompt_date("Warranty end date")
    else:
        warranty = None

    # Review and save
    sections = {
        "Device": {
            "Asset tag":      asset.get("asset_tag"),
            "Serial number":  asset.get("serial_number"),
            "Manufacturer":   asset.get("manufacturer"),
            "Model":          asset.get("model"),
            "Type":           asset.get("asset_type"),
            "Location":       asset.get("location"),
            "Department":     asset.get("department"),
            "Linked target":  asset.get("target_id"),
        },
        "Financial": {
            "Purchase date":  asset.get("purchase_date"),
            "Purchase price": f"${asset['purchase_price']:,.2f}" if asset.get("purchase_price") else None,
            "Useful life":    f"{asset.get('useful_life_years')} years",
            "Salvage value":  f"${float(asset.get('salvage_value', 0)):,.2f}",
        },
        "Warranty": {
            "Type":           warranty.get("warranty_type") if warranty else "None",
            "Vendor":         warranty.get("vendor") if warranty else None,
            "Contract #":     warranty.get("contract_number") if warranty else None,
            "Start":          warranty.get("start_date") if warranty else None,
            "End":            warranty.get("end_date") if warranty else None,
        },
    }

    if not confirm(sections):
        print("\n  Cancelled. Nothing was saved.\n")
        return

    asset_id = save_asset(asset, warranty)
    if asset_id:
        print(f"\n  Asset saved successfully (id={asset_id}).")
        if warranty and warranty.get("end_date"):
            from datetime import date
            end = date.fromisoformat(warranty["end_date"])
            days = (end - date.today()).days
            if days < 0:
                print(f"  !! Warranty already expired {abs(days)} days ago.")
            elif days < 90:
                print(f"  !! Warranty expires in {days} days — consider renewing.")
            else:
                print(f"  Warranty valid for {days} more days.")
        print()

    again = input("  Add another asset? (yes/no) [no]: ").strip().lower()
    if again in ("yes", "y"):
        run()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\n\n  Exited.\n")
        sys.exit(0)
