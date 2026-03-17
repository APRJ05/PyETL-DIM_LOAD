"""
etl/extractor.py — Paso 1: Extracción de datos (implementa IExtractor)

Aplica Clean Architecture:
  - Implementa IExtractor (Dependency Inversion)
  - Columnas requeridas leídas desde config.json (Single Responsibility)
  - Reintentos configurables por fuente (Open/Closed)
  - Stopwatch por archivo para métricas de rendimiento
"""

import os
import time
import pandas as pd
import config
from etl.interfaces import IExtractor
from utils.logger import get_logger, stopwatch

log = get_logger(__name__)


class Extractor(IExtractor):
    """
    Extrae datos desde archivos CSV.
    Implementa IExtractor — puede sustituirse por DatabaseExtractor
    o ApiExtractor sin cambiar el resto del pipeline.
    """

    def __init__(self):
        self.csv_dir  = config.CSV_DIR
        self.archivos = config.ARCHIVOS
        self.expected = config.EXPECTED_COLUMNS
        self.retries  = config.RETRY_ATTEMPTS
        self.delay    = config.RETRY_DELAY

    # ── IExtractor: extract ───────────────────────────────────

    def extract(self) -> dict[str, pd.DataFrame]:
        log.info("=" * 55)
        log.info("PASO 1 — EXTRACCIÓN")
        log.info("=" * 55)

        resultado = {}
        total_filas = 0

        for key, filename in self.archivos.items():
            path = os.path.join(self.csv_dir, filename)
            df = self._leer_con_reintentos(key, path)
            resultado[key] = df
            total_filas += len(df)
            log.info(
                f"  ✓ {filename:<28} → {len(df):>5} filas, "
                f"{len(df.columns)} columnas"
            )

        log.info(f"  Total archivos leídos: {len(resultado)}")
        log.info(f"  Total filas extraídas: {total_filas}")
        return resultado

    # ── IExtractor: validate ──────────────────────────────────

    def validate(self, data: dict[str, pd.DataFrame]) -> bool:
        """
        Verifica que cada DataFrame tenga las columnas mínimas requeridas
        según config.json → extraction.expected_columns.
        """
        ok = True
        for key, required_cols in self.expected.items():
            if key not in data:
                log.warning(f"  ⚠ Dataset '{key}' no encontrado en extracción.")
                ok = False
                continue
            df = data[key]
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                log.error(
                    f"  🔴 '{key}' — columnas faltantes: {missing}"
                )
                ok = False
            else:
                log.info(f"  ✓ '{key}' — estructura válida")
        return ok

    # ── Privados ──────────────────────────────────────────────

    def _leer_con_reintentos(self, key: str, path: str) -> pd.DataFrame:
        """
        Intenta leer el CSV hasta RETRY_ATTEMPTS veces antes de lanzar error.
        Implementa resiliencia ante errores transitorios de I/O.
        """
        ultimo_error = None
        for intento in range(1, self.retries + 1):
            try:
                with stopwatch(log, f"read {os.path.basename(path)}"):
                    return pd.read_csv(path, encoding="utf-8")
            except FileNotFoundError:
                log.error(f"  🔴 Archivo no encontrado: {path}")
                raise
            except Exception as e:
                ultimo_error = e
                log.warning(
                    f"  ⚠ Intento {intento}/{self.retries} fallido "
                    f"para '{key}': {e}"
                )
                if intento < self.retries:
                    time.sleep(self.delay)

        raise RuntimeError(
            f"No se pudo leer '{key}' tras {self.retries} intentos. "
            f"Último error: {ultimo_error}"
        )

    # ── Método run() para compatibilidad con pipeline ─────────

    def run(self) -> dict[str, pd.DataFrame]:
        """
        Punto de entrada principal: extrae y valida.
        Lanza ValueError si la validación falla.
        """
        data = self.extract()
        if not self.validate(data):
            raise ValueError(
                "Validación de estructura fallida. "
                "Revisa los archivos CSV y las columnas en config.json."
            )
        return data
