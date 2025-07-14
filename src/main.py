"""GoManage API Gateway (optimizado y corregido)"""
from __future__ import annotations

import json
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
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
load_dotenv()

class Config:
    BASE_URL: str = os.getenv("GOMANAGE_BASE_URL", "http://buyled.clonico.es:8181")
    USERNAME: str = os.getenv("GOMANAGE_USERNAME")
    PASSWORD: str = os.getenv("GOMANAGE_PASSWORD")
    AUTH_TOKEN: str = os.getenv("GOMANAGE_AUTH_TOKEN", "")
    CACHE_TTL: int = 7200  # segundos

# ---------------------------------------------------------------------------
# APP + LOG
# ---------------------------------------------------------------------------
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("gomanage-api")

# ---------------------------------------------------------------------------
# SESIÓN Y CACHÉ
# ---------------------------------------------------------------------------
_session_id: Optional[str] = None
_session_expires: Optional[datetime] = None
_customers_cache: List[Dict[str, Any]] = []


def _is_session_valid() -> bool:
    return bool(_session_id and _session_expires and _session_expires > datetime.utcnow())


def _authenticate() -> None:
    """Login y guarda JSESSIONID."""
    global _session_id, _session_expires
    url = f"{Config.BASE_URL}/gomanage/static/auth/j_spring_security_check"
    data = {"j_username": Config.USERNAME, "j_password": Config.PASSWORD}
    resp = requests.post(url, data=data, timeout=15, allow_redirects=False)
    resp.raise_for_status()
    cookie = resp.headers.get("Set-Cookie", "")
    for part in cookie.split(";"):
        if part.strip().startswith("JSESSIONID"):
            _session_id = part.split("=", 1)[1]
            _session_expires = datetime.utcnow() + timedelta(seconds=Config.CACHE_TTL)
            logger.info("Sesión GoManage establecida (expira %s)", _session_expires.isoformat())
            break
    else:
        raise RuntimeError("JSESSIONID no encontrado en la cabecera Set-Cookie")


def session_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not _is_session_valid():
            _authenticate()
        return func(*args, **kwargs)
    return wrapper


@session_required
def _request(method: str, endpoint: str, **kwargs) -> requests.Response:
    url = f"{Config.BASE_URL}{endpoint}"
    headers = {
        "Cookie": f"JSESSIONID={_session_id}",
        "Authorization": f"oecp {Config.AUTH_TOKEN}",
        "Accept": "application/json",
    }
    if kwargs.get("json") is not None:
        headers["Content-Type"] = "application/json"
    resp = requests.request(method, url, headers=headers, timeout=25, **kwargs)
    if resp.status_code == 401:
        _authenticate()
        headers["Cookie"] = f"JSESSIONID={_session_id}"
        resp = requests.request(method, url, headers=headers, timeout=25, **kwargs)
    resp.raise_for_status()
    return resp


def _load_paginated(endpoint: str) -> List[dict]:
    page, size, out = 1, 500, []
    while True:
        data = _request("GET", endpoint, params={"page": page, "size": size}).json()
        out.extend(data.get("page_entries", []))
        if len(out) >= data.get("total_entries", 0):
            break
        page += 1
    return out


def _ensure_customers_loaded() -> None:
    if not _customers_cache:
        logger.info("Descargando clientes …")
        _customers_cache.extend(_load_paginated("/gomanage/web/data/apitmt-customers/List"))
        logger.info("Clientes cargados: %d", len(_customers_cache))

# ---------------------------------------------------------------------------
# RUTAS
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("index_improved.html")


@app.route("/api/auth", methods=["POST"])
@session_required
def auth():
    _ensure_customers_loaded()
    return jsonify({
        "status": "success",
        "session_id": _session_id[:8] + "…" if _session_id else None,
        "customers_loaded": len(_customers_cache),
    })

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
    return jsonify({
        "customers": rows[start:end],
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": (total + per_page - 1) // per_page,
        },
    })


@app.route("/api/customers", methods=["POST"])
@session_required
def create_customer():
    payload = request.get_json(silent=True) or {}

    required = {"business_name", "name", "vat_number"}
    missing = required - payload.keys()
    if missing:
        return jsonify({"error": f"Campos obligatorios faltantes: {', '.join(missing)}"}), 400

    payload.setdefault("country_id", "ES")
    payload.setdefault("currency_id", "eur")
    payload.setdefault("language_id", "cas")

    try:
        resp = _request("POST", "/gomanage/web/data/apitmt-customers/", json=payload)
    except requests.HTTPError as exc:
        detail = exc.response.text if exc.response is not None else str(exc)
        logger.error("GoManage 4xx/5xx -> %s", detail[:400])
        return jsonify({"status": "error", "detail": detail}), exc.response.status_code if exc.response else 502

    _customers_cache.clear()
    return jsonify({"status": "success", "customer": resp.json()})

# -- Analytics ---------------------------------------------------------------
@app.route("/api/analytics/dashboard", methods=["GET"])
@session_required
def analytics_dashboard():
    _ensure_customers_loaded()
    by_type: Dict[str, int] = {}
    provinces: Dict[str, int] = {}
    for c in _customers_cache:
        by_type[c.get("tip_cli", "otros")] = by_type.get(c.get("tip_cli", "otros"), 0) + 1
        provinces[c.get("province_name", "Sin provincia")] = provinces.get(c.get("province_name", "Sin provincia"), 0) + 1
    top_provinces = dict(sorted(provinces.items(), key=lambda kv: kv[1], reverse=True)[:10])
    return jsonify({"customers": {"total": len(_customers_cache), "by_type": by_type, "top_provinces": top_provinces}})

# -- Chat MCP ----------------------------------------------------------------
@app.route("/api/chat/mcp", methods=["POST"])
@session_required
def chat_mcp():
    question = (request.get_json(silent=True) or {}).get("question", "").lower().strip()
    if not question:
        return jsonify({"error": "Pregunta vacía"}), 400
    if "cliente" in question:
        return jsonify({"response": f"Tenemos {len(_customers_cache)} clientes registrados"})
    return jsonify({"response": "No entiendo la pregunta"})

# ---------------------------------------------------------------------------
# Gunicorn importará `app`
# ---------------------------------------------------------------------------
