"""
Interactive target registration tool.

Run:
    python3 scripts/add_target.py
"""

import sys
import psycopg2
import psycopg2.extras

DB_DSN = "dbname=downtime_monitor"

# ---- Options presented to the user ----------------------------------------

TARGET_TYPES = [
    "pos",
    "kds",
    "server",
    "workstation",
    "switch",
    "ap",
    "camera",
    "printer",
    "tablet",
    "ups",
    "other",
]

PROTOCOLS = {
    "1": ("icmp", "Ping — use for anything with an IP (Pis, PCs, switches, cameras)"),
    "2": ("tcp",  "TCP port — use for services (Simphony, Prontos, TNG/BOS)"),
    "3": ("http", "HTTP/HTTPS — use for web interfaces or health endpoints"),
}

PRIORITIES = {
    "1": ("1", "Critical  — opens incident after 2 failures (Micros, Prontos, KDS)"),
    "2": ("2", "High      — opens incident after 3 failures (core switches, servers)"),
    "3": ("3", "Normal    — opens incident after 3 failures (workstations, APs)"),
    "4": ("4", "Low       — opens incident after 5 failures (printers, secondary devices)"),
}

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


# ---- Input helpers ---------------------------------------------------------

def prompt(label, default=None, required=True):
    suffix = f" [{default}]" if default is not None else ""
    while True:
        val = input(f"  {label}{suffix}: ").strip()
        if not val and default is not None:
            return default
        if not val and required:
            print("    ! This field is required.")
            continue
        return val or None


def prompt_choice(label, options: dict, default_key=None):
    """Present a numbered menu and return the chosen value."""
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
    """Present a numbered list and return the chosen string."""
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


def prompt_int(label, default=None, min_val=None, max_val=None):
    while True:
        raw = prompt(label, default=default)
        try:
            val = int(raw)
            if min_val is not None and val < min_val:
                print(f"    ! Minimum value is {min_val}")
                continue
            if max_val is not None and val > max_val:
                print(f"    ! Maximum value is {max_val}")
                continue
            return val
        except (TypeError, ValueError):
            print("    ! Please enter a whole number.")


def confirm(data):
    print("\n" + "=" * 55)
    print("  Review before saving")
    print("=" * 55)
    for k, v in data.items():
        if v is not None:
            print(f"  {k:<20} {v}")
    print("=" * 55)
    answer = input("\n  Save this target? (yes/no) [yes]: ").strip().lower() or "yes"
    return answer in ("yes", "y")


# ---- DB save ---------------------------------------------------------------

def save_target(data):
    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Check for duplicate name
            cur.execute("SELECT id FROM targets WHERE name = %s", (data["name"],))
            if cur.fetchone():
                print(f"\n  ! A target named '{data['name']}' already exists.")
                overwrite = input("  Add anyway with a different name? (yes/no) [no]: ").strip().lower()
                if overwrite not in ("yes", "y"):
                    return None

            cur.execute("""
                INSERT INTO targets
                    (name, hostname, ip_address, target_type, location,
                     department, priority, check_protocol, check_port,
                     check_path, check_interval, check_timeout, notes)
                VALUES
                    (%(name)s, %(hostname)s, %(ip_address)s, %(target_type)s,
                     %(location)s, %(department)s, %(priority)s, %(protocol)s,
                     %(port)s, %(path)s, %(interval)s, %(timeout)s, %(notes)s)
                RETURNING id
            """, data)
            return cur.fetchone()["id"]


# ---- Main flow -------------------------------------------------------------

def run():
    print("\n" + "=" * 55)
    print("  Add Monitoring Target")
    print("=" * 55)

    data = {}

    # Basic info
    print("\n  -- Device Info --")
    data["name"]        = prompt("Device name (e.g. Micros-Simphony-Main)")
    data["ip_address"]  = prompt("IP address")
    data["hostname"]    = prompt("Hostname (optional)", default=None, required=False)
    data["target_type"] = prompt_list("Device type", TARGET_TYPES)
    data["location"]    = prompt("Physical location (e.g. Server Room, Kitchen)", default="Resort")
    data["department"]  = prompt_list("Department", DEPARTMENTS)
    data["notes"]       = prompt("Notes (optional)", default=None, required=False)

    # Priority
    data["priority"] = int(prompt_choice(
        "Priority — how critical is this device?",
        PRIORITIES, default_key="3"
    ))

    # Check settings
    print("\n  -- Check Settings --")
    data["protocol"] = prompt_choice(
        "Check protocol — how should we monitor this device?",
        PROTOCOLS, default_key="1"
    )

    data["port"] = None
    data["path"] = None

    if data["protocol"] in ("tcp", "http"):
        data["port"] = prompt_int("Port number (e.g. 443, 80, 8080)", min_val=1, max_val=65535)

    if data["protocol"] == "http":
        data["path"] = prompt("URL path to check", default="/")

    # Suggest sensible defaults based on priority
    default_interval = 30 if data["priority"] <= 2 else 60
    default_timeout  = 5

    data["interval"] = prompt_int(f"Check interval in seconds", default=default_interval, min_val=10)
    data["timeout"]  = prompt_int(f"Timeout in seconds", default=default_timeout, min_val=1, max_val=60)

    # Confirm and save
    if not confirm(data):
        print("\n  Cancelled. Nothing was saved.\n")
        return

    target_id = save_target(data)
    if target_id:
        print(f"\n  Target '{data['name']}' added successfully (id={target_id}).")
        print(f"  The collector will start checking it on its next cycle.\n")

    # Add another?
    again = input("  Add another target? (yes/no) [no]: ").strip().lower()
    if again in ("yes", "y"):
        run()


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\n\n  Exited.\n")
        sys.exit(0)
