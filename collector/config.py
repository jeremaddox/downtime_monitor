import os

# Override any of these with environment variables in production.
DB_DSN = os.environ.get("DB_DSN", "dbname=downtime_monitor")
COLLECTOR_ID = os.environ.get("COLLECTOR_ID", "laptop")

# How many targets to check concurrently
THREAD_POOL_SIZE = int(os.environ.get("THREAD_POOL_SIZE", "20"))
