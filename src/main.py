"""GoManage API Gateway (optimised)
------------------------------------------------
Single‑file Flask application to proxy and enrich GoManage data.
• Env‑driven config (no credentials in code)
• Session caching with auto‑refresh
• Simple in‑memory cache for customers/products
• Structured logging (rather than prints)
• All routes unique; duplicates removed
• Ready for production with Gunicorn (no __main__)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS

# ---------------------------------------------------------------------------
# CONFIG --------------------------------------------------------------------
# ---------------------------------------------------------------------------
load_dotenv()

class Config:
    """Centralised settings (taken from env vars, with sane defaults)."""

    BASE_URL: str = os.getenv("GOMANAGE_BASE_URL", "http://localhost:8181")
    USERNAME: str = os.getenv("GOMANAGE_USERNAME", "user")
    PASSWORD: str = os.getenv("GOMANAGE_PASSWORD", "pass")
    AUTH_TOKEN: str = os.getenv("GOMANAGE_AUTH_TOKEN", "")

    # Request time‑outs
    CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "10"))
    READ_TIMEOUT = float(os.getenv("READ_TIMEOUT", "25"))

    # Caching
    CACHE_TTL = int(os.getenv("CACHE_TTL", "7200"))  # 2 h


# ---------------------------------------------------------------------------
# APP SET‑UP -----------------------------------------------------------------
# ---------------------------------------------------------------------------
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Logger configured once
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("gomanage-api")


# ---------------------------------------------------------------------------
# SESSION / CACHE ------------------------------------------------------------
# ---------------------------------------------------------------------------
_session_id: Optional[str] = None
_session_expires: Optional[datetime] = None
_customers_cache: List[Dict[str, Any]] = []
_products_cache: List[Dict[str, Any]] = []


def session_required(func):
    """Decorator: ensures we have a valid JSESSIONID before calling func."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        if not _is_session_valid():
            _authenticate()
        return func(*args, **kwargs)

    return wrapper


def _is_session_valid() -> bool:
    return _session_id is not None and _session_expires and _session_expires > datetime.utcnow()


def _authenticate() -> None:
    """Perform form‑based login to GoManage and store JSESSIONID."""
    global _session_id, _session_expires

    auth_url = f"{Config.BASE_URL}/gomanage/static/auth/j_spring_security_check"
    data = {"j_username": Config.USERNAME, "j_password": Config.PASSWORD}

    logger.info("Authenticating to GoManage as %s", Config.USERNAME)
    try:
        resp = requests.post(auth_url, data=data, timeout=(Config.CONNECT_TIMEOUT, Config.READ_TIMEOUT), allow_redirects=False)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Could not authenticate: %s", exc)
        raise

    # Parse cookie header
    cookie_header = resp.headers.get("Set-Cookie", "")
    for part in cookie_header.split(";"):
        if part.strip().startswith("JSESSIONID"):
            _session_id = part.split("=", 1)[1]
            _session_expires = datetime.utcnow() + timedelta(seconds=Config.CACHE_TTL)
            logger.info("Session established, expires at %s", _session_expires.isoformat(timespec="seconds"))
            break
    else:
        raise RuntimeError("JSESSIONID cookie not found in auth response")


@session_required
def _request(method: str, endpoint: str, *, params: dict | None = None, json: dict | None = None) -> requests.Response:
    """Wrapper around requests with session & auth headers."""
    url = f"{Config.BASE_URL}{endpoint}"
    headers = {
        "Cookie": f"JSESSIONID={_session_id}",
        "Authorization": f"oecp {Config.AUTH_TOKEN}",
        "Accept": "application/json",
    }
    if json is not None:
        headers["Content-Type"] = "application/json"

    request_kwargs = dict(headers=headers, params=params, json=json, timeout=(Config.CONNECT_TIMEOUT, Config.READ_TIMEOUT))

    response = requests.request(method.upper(), url, **request_kwargs)
    if response.status_code == 401:
        # Session expired → re‑authenticate once and retry
        logger.info("401 received, refreshing session and retrying …")
        _authenticate()
        headers["Cookie"] = f"JSESSIONID={_session_id}"
        response = requests.request(method.upper(), url, **request_kwargs)
    response.raise_for_status()
    return response


# ---------------------------------------------------------------------------
# DATA LOADING / CACHING -----------------------------------------------------
# ---------------------------------------------------------------------------

def _load_paginated(endpoint: str, page_key: str) -> List[dict]:
    """Generic helper to fetch *all* pages from a GoManage list endpoint."""
    page_size = 500
    page = 1
    output: List[dict] = []

    while True:
        params = {"page": page, "size": page_size}
        resp = _request("GET", endpoint, params=params)
        data = resp.json()
        entries = data.get("page_entries", [])
        output.extend(entries)
        if len(output) >= data.get("total_entries", 0):
            break
        page += 1
    return output


def _ensure_customers_loaded():
    if not _customers_cache:
        logger.info("Downloading customer catalogue …")
        _customers_cache.extend(_load_paginated("/gomanage/web/data/apitmt-customers/List", "customers"))
        logger.info("Loaded %d customers", len(_customers_cache))


def _ensure_products_loaded():
    if not _products_cache:
        logger.info("Downloading product catalogue …")
        _products_cache.extend(_load_paginated("/gomanage/web/data/apitmt-products/List", "products"))
        logger.info("Loaded %d products", len(_products_cache))


# ---------------------------------------------------------------------------
# ROUTES ---------------------------------------------------------------------
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("index_improved.html")


# -- Auth --------------------------------------------------------------------
@app.route("/api/auth", methods=["POST"])
@session_required
def auth():
    _ensure_customers_loaded()
    _ensure_products_loaded()
    return jsonify(
        {
            "status": "success",
            "session_id": _session_id[:8] + "…" if _session_id else None,
            "customers_loaded": len(_customers_cache),
            "products_loaded": len(_products_cache),
        }
    )


# -- Customers ---------------------------------------------------------------
@app.route("/api/customers", methods=["GET"])
@session_required
def get_customers():
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    search = request.args.get("search", "").lower().strip()

    _ensure_customers_loaded()
    rows = _customers_cache

    if search:
        rows = [c for c in rows if search in json.dumps(c).lower()]

    total = len(rows)
    start, end = (page - 1) * per_page, page * per_page
    return jsonify(
        {
            "customers": rows[start:end],
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": (total + per_page - 1) // per_page,
            },
        }
    )


# -- Analytics ---------------------------------------------------------------
@app.route("/api/analytics/dashboard", methods=["GET"])
@session_required
def analytics_dashboard():
    _ensure_customers_loaded()

    customer_types: Dict[str, int] = {}
    provinces: Dict[str, int] = {}
    for cust in _customers_cache:
        customer_types[cust.get("tip_cli", "otros")] = customer_types.get(cust.get("tip_cli", "otros"), 0) + 1
        provinces[cust.get("province_name", "Sin provincia")] = provinces.get(cust.get("province_name", "Sin provincia"), 0) + 1

    top_provinces = dict(sorted(provinces.items(), key=lambda kv: kv[1], reverse=True)[:10])

    # lightweight: request just first page of invoices for total
    sales_resp = _request("GET", "/gomanage/web/data/apitmt-sales-invoices/List", params={"size": 100})
    invoices = sales_resp.json().get("page_entries", [])
    total_sales = sum(float(inv.get("total", 0)) for inv in invoices)

    return jsonify(
        {
            "customers": {
                "total": len(_customers_cache),
                "by_type": customer_types,
                "top_provinces": top_provinces,
            },
            "sales": {
                "sample": len(invoices),
                "total_sample_amount": total_sales,
            },
        }
    )


# -- Chat MCP (simple rules) --------------------------------------------------
@app.route("/api/chat/mcp", methods=["POST"])
@session_required
def chat_mcp():
    data = request.get_json(silent=True) or {}
    question = data.get("question", "").strip()
    if not question:
        return jsonify({"error": "Pregunta vacía"}), 400
    return jsonify({"response": _mcp_answer(question), "timestamp": datetime.utcnow().isoformat()})


def _mcp_answer(text: str) -> str:
    t = text.lower()
    if "cliente" in t:
        return f"Tenemos {len(_customers_cache)} clientes en la base de datos."
    if "ventas" in t or "pedido" in t:
        return "Consulta /api/analytics/dashboard para ver estadísticas de ventas."
    return "No entiendo la pregunta, prueba con 'clientes' o 'ventas'."


# ---------------------------------------------------------------------------
# Production entry‑point (Gunicorn will look for `app`) -----------------------
# ---------------------------------------------------------------------------
# No `if __name__ == '__main__'`: run locally with `flask run` or
# `python -m flask run --app gomanage_api.py
