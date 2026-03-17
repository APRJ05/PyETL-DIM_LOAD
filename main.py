"""
main.py — Orquestador del pipeline ETL (Clean Architecture)

Coordina las 4 etapas del pipeline sin depender de implementaciones
concretas — solo de las interfaces IExtractor, ITransformer, ILoader.

Principios aplicados:
  - Dependency Inversion: main depende de interfaces, no de clases
  - Single Responsibility: cada clase hace una sola cosa
  - Configuración centralizada en config.json
  - Métricas de rendimiento con Stopwatch y MetricsCollector
"""

import sys
import os
import time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from etl.extractor    import Extractor
from etl.transformer  import Transformer
from etl.loader       import Loader
from etl.loader_star  import LoaderStar
from utils.logger     import get_logger, MetricsCollector

log     = get_logger("main")
metrics = MetricsCollector()


def run_stage(name: str, fn):
    """Ejecuta una etapa, mide tiempo y registra métricas."""
    t0 = time.perf_counter()
    try:
        result = fn()
        elapsed = time.perf_counter() - t0
        metrics.record_time(name, elapsed)
        if isinstance(result, dict):
            total = sum(len(df) for df in result.values())
            metrics.record_count(name, total)
        return result
    except Exception as e:
        metrics.record_error(name)
        raise


def main():
    t0 = datetime.now()
    log.info("=" * 55)
    log.info(f"INICIO DEL PIPELINE: {t0.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    try:
        # ── Paso 1: Extracción ─────────────────────────────────
        raw = run_stage("1-Extraccion",
            lambda: Extractor().run())

        # ── Paso 2: Transformación ─────────────────────────────
        limpio = run_stage("2-Transformacion",
            lambda: Transformer().run(raw))

        # ── Paso 3: Carga OLTP ────────────────────────────────
        run_stage("3-Carga-OLTP",
            lambda: Loader().run(limpio) or {})

        # ── Paso 4: Carga OLAP ────────────────────────────────
        run_stage("4-Carga-OLAP",
            lambda: LoaderStar().run() or {})

    except Exception as e:
        log.critical(f"Pipeline terminado con error: {e}")
        sys.exit(1)
    finally:
        t1 = datetime.now()
        log.info(f"Fin: {t1.strftime('%Y-%m-%d %H:%M:%S')}  |  "
                 f"Duración total: {t1 - t0}")
        metrics.summary(log)


if __name__ == "__main__":
    main()
