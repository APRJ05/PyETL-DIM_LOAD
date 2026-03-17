"""
utils/logger.py — Logging estructurado con métricas de rendimiento
"""

import logging
import time
import config
from contextlib import contextmanager


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level = getattr(logging, config.LOG_LEVEL, logging.INFO)
    logger.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    try:
        fh = logging.FileHandler(config.LOG_FILE, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except Exception:
        pass

    return logger


@contextmanager
def stopwatch(logger: logging.Logger, label: str):
    """
    Context manager que mide tiempo de ejecución de un bloque.
    Uso:
        with stopwatch(log, "Extracción CSV"):
            datos = extractor.extract()
    """
    t0 = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - t0
        logger.info(f"  ⏱  {label:<30} {elapsed:.4f}s")


class MetricsCollector:
    """
    Acumula métricas del pipeline: tiempo, registros, errores y reintentos.
    """

    def __init__(self):
        self._times:   dict[str, float] = {}
        self._records: dict[str, int]   = {}
        self._errors:  dict[str, int]   = {}
        self._retries: dict[str, int]   = {}

    def record_time(self, stage: str, seconds: float):
        self._times[stage] = seconds

    def record_count(self, stage: str, count: int):
        self._records[stage] = count

    def record_error(self, stage: str):
        self._errors[stage] = self._errors.get(stage, 0) + 1

    def record_retry(self, stage: str):
        self._retries[stage] = self._retries.get(stage, 0) + 1

    def summary(self, logger: logging.Logger):
        logger.info("=" * 55)
        logger.info("MÉTRICAS DE EJECUCIÓN")
        logger.info("=" * 55)
        for stage, secs in self._times.items():
            records = self._records.get(stage, "-")
            errors  = self._errors.get(stage, 0)
            retries = self._retries.get(stage, 0)
            logger.info(
                f"  {stage:<22} {secs:.4f}s  |  "
                f"registros={records}  errores={errors}  reintentos={retries}"
            )
        logger.info("=" * 55)
