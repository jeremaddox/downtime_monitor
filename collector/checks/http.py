"""
HTTP/HTTPS check — GET request, success if status code < 400.
"""

import time
import requests


def check(host: str, port: int = None, path: str = "/",
          timeout: int = 10) -> dict:
    """
    Returns: {"success": bool, "latency_ms": float|None,
              "status_code": int|None, "error": str|None}
    """
    scheme = "https" if port == 443 else "http"
    port_part = f":{port}" if port and port not in (80, 443) else ""
    url = f"{scheme}://{host}{port_part}{path or '/'}"

    start = time.monotonic()
    try:
        resp = requests.get(url, timeout=timeout, allow_redirects=True,
                            verify=False)
        latency = (time.monotonic() - start) * 1000
        success = resp.status_code < 400
        return {
            "success": success,
            "latency_ms": round(latency, 3),
            "status_code": resp.status_code,
            "error": None if success else f"HTTP {resp.status_code}",
        }
    except requests.exceptions.Timeout:
        return {"success": False, "latency_ms": None,
                "status_code": None, "error": "timeout"}
    except requests.exceptions.ConnectionError as e:
        return {"success": False, "latency_ms": None,
                "status_code": None, "error": str(e)}
    except Exception as e:
        return {"success": False, "latency_ms": None,
                "status_code": None, "error": str(e)}
