# Downtime Monitor

Operational downtime monitoring and actuarial loss modeling system for a resort IT environment.

Monitors network devices and servers via ICMP/TCP/HTTP, tracks weather alerts from the National Weather Service, correlates outages with weather events, and generates actuarial forecasts of expected downtime and revenue loss.

---

## Architecture

```
collector/      — probe agents (ICMP, TCP, HTTP) + NWS weather poller
engine/         — correlator: links incidents to weather alerts
reports/        — actuarial loss & weather outage reports
db/             — PostgreSQL schema
scripts/        — seed scripts for lab/test devices
tests/          — pytest schema and data round-trip tests
```

---

## Requirements

- Python 3.10+
- PostgreSQL 14+

---

## Setup

### 1. Clone the repo

```bash
git clone git@github.com:jeremaddox/downtime_monitor.git
cd downtime_monitor
```

### 2. Install Python dependencies

```bash
pip3 install -r requirements.txt --break-system-packages
```

### 3. Create the database

```bash
sudo -u postgres createdb downtime_monitor
sudo -u postgres createuser --superuser $USER
```

### 4. Load the schema

```bash
psql downtime_monitor < db/schema.sql
psql downtime_monitor < db/weather_schema.sql
```

### 5. Seed lab devices

Edit `scripts/seed_lab.py` and update the IP addresses for your devices, then run:

```bash
python3 scripts/seed_lab.py
```

---

## Running

### Monitor devices (checks every 30–60 seconds per target)

```bash
python3 -m collector.collector
```

### Poll weather from NWS — Pine Mountain, GA (every 15 minutes)

```bash
python3 -m collector.weather
```

### Link incidents to weather alerts

Run this after incidents have accumulated:

```bash
python3 -m engine.correlator
```

### Generate the actuarial report

```bash
python3 -m reports.weather_outage_report
```

Save to a file:

```bash
python3 -m reports.weather_outage_report --out reports/output/report_$(date +%F).txt
```

Output as JSON:

```bash
python3 -m reports.weather_outage_report --json
```

---

## Running Tests

```bash
python3 -m pytest tests/test_schema.py -v
```

Tests use savepoints — nothing is written to the database permanently.

---

## Lab Devices (current)

| Device           | IP             | Protocol |
|------------------|----------------|----------|
| Laptop           | 192.168.1.54   | ICMP     |
| RPi-5            | 192.168.1.201  | ICMP     |
| RPi-4            | 192.168.1.202  | ICMP     |
| RPi-Zero-1       | 192.168.1.231  | ICMP     |
| RPi-Zero-2       | 192.168.1.232  | ICMP     |
| RPi-Zero-3       | 192.168.1.233  | ICMP     |
| RPi-Zero-4       | 192.168.1.234  | ICMP     |
| HP ProCurve 2520 | 192.168.1.2    | ICMP     |

---

## Database Tables

| Table                  | Purpose                                          |
|------------------------|--------------------------------------------------|
| `targets`              | Monitored devices and services                   |
| `raw_checks`           | Every probe result                               |
| `target_state`         | Current status per target                        |
| `incidents`            | Downtime periods (open until resolved)           |
| `tickets`              | Help-desk tickets linked to incidents            |
| `loss_parameters`      | Financial parameters for loss modeling           |
| `assets`               | Physical hardware inventory                      |
| `asset_events`         | Repairs, replacements, and lifecycle events      |
| `asset_parameters`     | Per-asset actuarial parameters (MTTR, etc.)      |
| `warranty_events`      | Warranty registrations and claims                |
| `weather_observations` | NWS station KPIM observations                    |
| `weather_alerts`       | NWS active alerts for Harris County, GA          |
| `incident_weather`     | Links incidents to concurrent weather alerts     |

---

## Weather

Uses the [National Weather Service API](https://www.weather.gov/documentation/services-web-api) — no API key required.

- **Station:** KPIM — Pine Mountain Harris County Airport
- **Alerts zone:** GAZ078 (Harris County forecast zone) + GAC145 (Harris County)

---

## Actuarial Report

The report uses a **Poisson distribution** to model outage frequency. Given historical incident rates per weather event type, it forecasts:

- Expected number of incidents over the next 12 months
- 80% confidence interval (10th–90th percentile)
- Estimated revenue at risk based on configurable `revenue_per_hour` loss parameter

To update the revenue/hour used in forecasts:

```sql
UPDATE loss_parameters
SET effective_to = CURRENT_DATE
WHERE param_key = 'revenue_per_hour' AND effective_to IS NULL;

INSERT INTO loss_parameters (param_scope, param_key, param_value, description)
VALUES ('global', 'revenue_per_hour', 10000, 'Updated resort avg revenue/hr');
```
