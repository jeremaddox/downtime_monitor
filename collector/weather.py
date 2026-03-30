"""
Weather collector for Pine Mountain, GA.

Polls the NWS API every 15 minutes:
  - Observations from station KPIM
  - Active alerts for Harris County

Run standalone:
    python3 -m collector.weather

Or import run() and call it in a thread.
"""

import logging
import time

import psycopg2
import requests

from collector.config import DB_DSN

log = logging.getLogger(__name__)

# NWS endpoints — Pine Mountain, GA
STATION_ID       = "KPIM"
OBSERVATIONS_URL = f"https://api.weather.gov/stations/{STATION_ID}/observations/latest"
ALERTS_URL       = "https://api.weather.gov/alerts/active?zone=GAZ078,GAC145"

HEADERS = {"User-Agent": "downtime-monitor/1.0 (resort IT lab)"}
POLL_INTERVAL = 900   # 15 minutes


# ---------------------------------------------------------------------------
# Unit conversions
# ---------------------------------------------------------------------------

def c_to_f(c):
    if c is None:
        return None
    return round(c * 9 / 5 + 32, 1)

def ms_to_mph(ms):
    if ms is None:
        return None
    return round(ms * 2.23694, 1)

def pa_to_inhg(pa):
    if pa is None:
        return None
    return round(pa * 0.0002953, 2)

def m_to_miles(m):
    if m is None:
        return None
    return round(m / 1609.344, 2)

def mm_to_in(mm):
    if mm is None:
        return None
    return round(mm / 25.4, 3)

def val(obj):
    """Extract .value from an NWS quantity object, or None."""
    if obj is None:
        return None
    return obj.get("value")


# ---------------------------------------------------------------------------
# Fetch & store observation
# ---------------------------------------------------------------------------

def fetch_observation():
    resp = requests.get(OBSERVATIONS_URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    props = resp.json()["properties"]

    observed_at     = props.get("timestamp")
    temp_f          = c_to_f(val(props.get("temperature")))
    dewpoint_f      = c_to_f(val(props.get("dewpoint")))
    humidity_pct    = val(props.get("relativeHumidity"))
    wind_speed_mph  = ms_to_mph(val(props.get("windSpeed")))
    wind_gust_mph   = ms_to_mph(val(props.get("windGust")))
    wind_dir        = val(props.get("windDirection"))
    pressure_inhg   = pa_to_inhg(val(props.get("seaLevelPressure")))
    precip_1h_in    = mm_to_in(val(props.get("precipitationLastHour")))
    visibility      = m_to_miles(val(props.get("visibility")))
    condition       = props.get("textDescription")

    return {
        "observed_at":         observed_at,
        "station_id":          STATION_ID,
        "temp_f":              temp_f,
        "dewpoint_f":          dewpoint_f,
        "humidity_pct":        humidity_pct,
        "wind_speed_mph":      wind_speed_mph,
        "wind_gust_mph":       wind_gust_mph,
        "wind_direction_deg":  wind_dir,
        "pressure_inhg":       pressure_inhg,
        "precip_1h_in":        precip_1h_in,
        "visibility_miles":    visibility,
        "condition":           condition,
        "raw_text":            condition,
    }


def store_observation(cur, obs):
    cur.execute("""
        INSERT INTO weather_observations
            (observed_at, station_id, temp_f, dewpoint_f, humidity_pct,
             wind_speed_mph, wind_gust_mph, wind_direction_deg, pressure_inhg,
             precip_1h_in, visibility_miles, condition, raw_text)
        VALUES
            (%(observed_at)s, %(station_id)s, %(temp_f)s, %(dewpoint_f)s,
             %(humidity_pct)s, %(wind_speed_mph)s, %(wind_gust_mph)s,
             %(wind_direction_deg)s, %(pressure_inhg)s, %(precip_1h_in)s,
             %(visibility_miles)s, %(condition)s, %(raw_text)s)
        ON CONFLICT (station_id, observed_at) DO NOTHING
    """, obs)
    log.info("Observation: %s°F  %s  wind=%smph  precip=%sin",
             obs["temp_f"], obs["condition"],
             obs["wind_speed_mph"], obs["precip_1h_in"])


# ---------------------------------------------------------------------------
# Fetch & store alerts
# ---------------------------------------------------------------------------

def fetch_alerts():
    resp = requests.get(ALERTS_URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.json().get("features", [])


def store_alerts(cur, features):
    active_ids = set()
    for f in features:
        p = f["properties"]
        nws_id = p.get("id")
        if not nws_id:
            continue
        active_ids.add(nws_id)
        cur.execute("""
            INSERT INTO weather_alerts
                (nws_id, event_type, severity, urgency, headline,
                 description, effective_at, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (nws_id) DO UPDATE SET
                expires_at  = EXCLUDED.expires_at,
                description = EXCLUDED.description
        """, (
            nws_id,
            p.get("event"),
            p.get("severity"),
            p.get("urgency"),
            p.get("headline"),
            p.get("description"),
            p.get("effective") or p.get("sent"),
            p.get("expires"),
        ))
        log.info("Alert: [%s] %s — %s", p.get("severity"), p.get("event"), p.get("headline", "")[:80])

    # Mark alerts no longer in active feed as ended
    if active_ids:
        cur.execute("""
            UPDATE weather_alerts
            SET ended_at = NOW()
            WHERE ended_at IS NULL
              AND expires_at < NOW()
        """)
    else:
        # No active alerts — close any that have passed expiry
        cur.execute("""
            UPDATE weather_alerts SET ended_at = NOW()
            WHERE ended_at IS NULL AND expires_at < NOW()
        """)

    if not features:
        log.info("No active weather alerts.")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run():
    log.info("Weather collector starting  [station=%s  interval=%ds]",
             STATION_ID, POLL_INTERVAL)
    while True:
        try:
            obs     = fetch_observation()
            alerts  = fetch_alerts()

            with psycopg2.connect(DB_DSN) as conn:
                with conn.cursor() as cur:
                    store_observation(cur, obs)
                    store_alerts(cur, alerts)

        except requests.exceptions.RequestException as e:
            log.error("NWS API error: %s", e)
        except Exception as e:
            log.exception("Weather collector error: %s", e)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-8s  %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    run()
