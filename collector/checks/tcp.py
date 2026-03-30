"""
TCP check — attempts to open a socket to host:port.
"""

import socket
import time


def check(host: str, port: int, timeout: int = 10) -> dict:
    """
    Returns: {"success": bool, "latency_ms": float|None, "error": str|None}
    """
    start = time.monotonic()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            latency = (time.monotonic() - start) * 1000
            return {"success": True, "latency_ms": round(latency, 3), "error": None}
    except socket.timeout:
        return {"success": False, "latency_ms": None, "error": "timeout"}
    except ConnectionRefusedError:
        return {"success": False, "latency_ms": None, "error": "connection refused"}
    except Exception as e:
        return {"success": False, "latency_ms": None, "error": str(e)}
