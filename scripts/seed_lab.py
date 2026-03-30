"""
Seed lab devices as targets in the database.

Edit the IP addresses below to match your network, then run:
    python3 scripts/seed_lab.py
"""

import psycopg2
import psycopg2.extras
import sys

DB_DSN = "dbname=downtime_monitor"

# ---------------------------------------------------------------------------
# Edit these IPs to match your lab network
# ---------------------------------------------------------------------------
LAB_TARGETS = [
    {
        "name":           "Laptop",
        "hostname":       "localhost",
        "ip_address":     "192.168.1.54",
        "target_type":    "workstation",
        "location":       "Lab",
        "department":     "IT",
        "priority":       3,
        "check_protocol": "icmp",
        "check_port":     None,
        "check_interval": 30,
        "check_timeout":  5,
    },
    {
        "name":           "RPi-5",
        "hostname":       "rpi5.local",
        "ip_address":     "192.168.1.201",
        "target_type":    "server",
        "location":       "Lab",
        "department":     "IT",
        "priority":       2,
        "check_protocol": "icmp",
        "check_port":     None,
        "check_interval": 30,
        "check_timeout":  5,
    },
    {
        "name":           "RPi-4",
        "hostname":       "rpi4.local",
        "ip_address":     "192.168.1.202",
        "target_type":    "server",
        "location":       "Lab",
        "department":     "IT",
        "priority":       2,
        "check_protocol": "icmp",
        "check_port":     None,
        "check_interval": 30,
        "check_timeout":  5,
    },
    {
        "name":           "RPi-Zero-1",
        "hostname":       "rpizero1.local",
        "ip_address":     "192.168.1.20",   # <-- update
        "target_type":    "server",
        "location":       "Lab",
        "department":     "IT",
        "priority":       3,
        "check_protocol": "icmp",
        "check_port":     None,
        "check_interval": 60,
        "check_timeout":  10,
    },
    {
        "name":           "RPi-Zero-2",
        "hostname":       "rpizero2.local",
        "ip_address":     "192.168.1.21",   # <-- update
        "target_type":    "server",
        "location":       "Lab",
        "department":     "IT",
        "priority":       3,
        "check_protocol": "icmp",
        "check_port":     None,
        "check_interval": 60,
        "check_timeout":  10,
    },
    {
        "name":           "RPi-Zero-3",
        "hostname":       "rpizero3.local",
        "ip_address":     "192.168.1.22",   # <-- update
        "target_type":    "server",
        "location":       "Lab",
        "department":     "IT",
        "priority":       3,
        "check_protocol": "icmp",
        "check_port":     None,
        "check_interval": 60,
        "check_timeout":  10,
    },
    {
        "name":           "RPi-Zero-4",
        "hostname":       "rpizero4.local",
        "ip_address":     "192.168.1.23",   # <-- update
        "target_type":    "server",
        "location":       "Lab",
        "department":     "IT",
        "priority":       3,
        "check_protocol": "icmp",
        "check_port":     None,
        "check_interval": 60,
        "check_timeout":  10,
    },
    {
        "name":           "HP-ProCurve-2520",
        "hostname":       "switch.local",
        "ip_address":     "192.168.1.2",
        "target_type":    "switch",
        "location":       "Lab",
        "department":     "IT",
        "priority":       1,
        "check_protocol": "icmp",
        "check_port":     None,
        "check_interval": 30,
        "check_timeout":  5,
    },
]


def seed():
    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            inserted = 0
            skipped  = 0
            for t in LAB_TARGETS:
                cur.execute(
                    "SELECT id FROM targets WHERE name = %s", (t["name"],)
                )
                if cur.fetchone():
                    print(f"  SKIP  {t['name']} (already exists)")
                    skipped += 1
                    continue

                cur.execute("""
                    INSERT INTO targets
                        (name, hostname, ip_address, target_type, location, department,
                         priority, check_protocol, check_port, check_interval, check_timeout)
                    VALUES
                        (%(name)s, %(hostname)s, %(ip_address)s, %(target_type)s,
                         %(location)s, %(department)s, %(priority)s, %(check_protocol)s,
                         %(check_port)s, %(check_interval)s, %(check_timeout)s)
                    RETURNING id
                """, t)
                row = cur.fetchone()
                print(f"  ADD   {t['name']}  (id={row['id']})")
                inserted += 1

    print(f"\nDone — {inserted} added, {skipped} skipped.")


if __name__ == "__main__":
    seed()
