"""
ICMP check — runs a single ping and returns (success, latency_ms, error).
Uses the system ping binary so no root privileges are needed.
"""

import subprocess
import re
import time


def check(host: str, timeout: int = 10) -> dict:
    """
    Ping host once. Returns:
        {"success": bool, "latency_ms": float|None, "error": str|None}
    """
    try:
        result = subprocess.run(
            ["ping", "-c", "1", "-W", str(timeout), host],
            capture_output=True,
            text=True,
            timeout=timeout + 2,
        )

        if result.returncode == 0:
            # Extract rtt from line like: rtt min/avg/max/mdev = 0.123/0.123/0.123/0.000 ms
            match = re.search(r"rtt .+ = [\d.]+/([\d.]+)/", result.stdout)
            latency = float(match.group(1)) if match else None
            return {"success": True, "latency_ms": latency, "error": None}
        else:
            return {"success": False, "latency_ms": None,
                    "error": result.stderr.strip() or "ping failed"}

    except subprocess.TimeoutExpired:
        return {"success": False, "latency_ms": None, "error": "timeout"}
    except Exception as e:
        return {"success": False, "latency_ms": None, "error": str(e)}
