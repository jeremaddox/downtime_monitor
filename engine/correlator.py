"""
Correlator — links incidents to weather alerts that were active
when the incident started.

Run after incidents are closed, or on a schedule:
    python3 -m engine.correlator
"""

import logging
import psycopg2

from collector.config import DB_DSN

log = logging.getLogger(__name__)


def correlate():
    """
    For every incident not yet correlated, find weather alerts that
    overlapped its start time and insert into incident_weather.
    """
    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor() as cur:

            # Find incidents with no weather correlation yet
            cur.execute("""
                SELECT i.id, i.target_id, i.started_at
                FROM incidents i
                WHERE NOT EXISTS (
                    SELECT 1 FROM incident_weather iw WHERE iw.incident_id = i.id
                )
            """)
            incidents = cur.fetchall()

            linked = 0
            for inc_id, target_id, started_at in incidents:
                # Find alerts active at the moment the incident started
                cur.execute("""
                    SELECT id FROM weather_alerts
                    WHERE effective_at <= %s
                      AND (expires_at >= %s OR expires_at IS NULL)
                """, (started_at, started_at))
                alert_ids = [row[0] for row in cur.fetchall()]

                for alert_id in alert_ids:
                    cur.execute("""
                        INSERT INTO incident_weather (incident_id, alert_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                    """, (inc_id, alert_id))
                    linked += 1

    log.info("Correlator: linked %d incident-alert pairs.", linked)
    return linked


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s  %(levelname)-8s  %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    correlate()
