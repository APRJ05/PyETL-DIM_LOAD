"""
config.py — Carga centralizada de configuración desde config.json
Single source of truth para todo el pipeline.
"""

import json
import os

_BASE = os.path.dirname(os.path.abspath(__file__))
_CFG_PATH = os.path.join(_BASE, "config.json")

with open(_CFG_PATH, "r", encoding="utf-8") as f:
    _cfg = json.load(f)

# ── OLTP ──────────────────────────────────────────────────────
SERVER           = _cfg["database"]["oltp"]["server"]
DATABASE         = _cfg["database"]["oltp"]["database"]
USE_WINDOWS_AUTH = _cfg["database"]["oltp"]["windows_auth"]
SQL_USER         = _cfg["database"]["oltp"]["sql_user"]
SQL_PASS         = _cfg["database"]["oltp"]["sql_pass"]

# ── OLAP ──────────────────────────────────────────────────────
DW_SERVER        = _cfg["database"]["olap"]["server"]
DW_DATABASE      = _cfg["database"]["olap"]["database"]

# ── Extracción ────────────────────────────────────────────────
BASE_DIR         = _BASE
CSV_DIR          = os.path.join(_BASE, _cfg["extraction"]["csv_dir"])
ARCHIVOS         = _cfg["extraction"]["files"]
EXPECTED_COLUMNS = _cfg["extraction"]["expected_columns"]
RETRY_ATTEMPTS   = _cfg["extraction"]["retry_attempts"]
RETRY_DELAY      = _cfg["extraction"]["retry_delay_seconds"]

# ── Logging ───────────────────────────────────────────────────
LOG_LEVEL        = _cfg["logging"]["level"]
LOG_FILE         = os.path.join(_BASE, _cfg["logging"]["log_file"])
