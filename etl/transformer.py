"""
Limpieza, normalización y validación (implementa ITransformer)
"""

import random
import pandas as pd
from etl.interfaces import ITransformer
from utils.logger import get_logger

log = get_logger(__name__)

# Datos para simulación de campos de cliente
_PAISES = [
    "México", "Colombia", "Argentina", "Chile", "Perú",
    "España", "Ecuador", "Venezuela", "Guatemala", "Bolivia"
]
_TIPOS = ["Regular", "Premium", "VIP"]

random.seed(42)  # reproducible

"""Los CSV no venían con estos datos pero la db debía poder responder esas preguntas"""

class Transformer(ITransformer):
    """
    Aplica limpieza y normalización a cada DataFrame.
    Cada método privado maneja un dataset específico.
    """

    def run(self, raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        log.info("=" * 55)
        log.info("PASO 2 — TRANSFORMACIÓN Y LIMPIEZA")
        log.info("=" * 55)

        return {
            "clientes":   self._clientes(raw["clientes"]),
            "productos":  self._productos(raw["productos"]),
            "fuentes":    self._fuentes(raw["fuentes"]),
            "survey":     self._survey(raw["survey"]),
            "social":     self._social(raw["social"]),
            "webreviews": self._webreviews(raw["webreviews"]),
        }

    # ── Helpers ───────────────────────────────────────────────

    @staticmethod
    def _quitar_prefijo(serie: pd.Series) -> pd.Series:
        """Elimina prefijos no numéricos: C019 → 19, P003 → 3"""
        return serie.str.replace(r"[^\d]", "", regex=True)

    @staticmethod
    def _a_fecha(serie: pd.Series) -> pd.Series:
        return pd.to_datetime(serie, errors="coerce").dt.date

    @staticmethod
    def _reporte(nombre: str, antes: int, despues: int):
        descartados = antes - despues
        msg = f"  {nombre:<20} {antes} → {despues} filas limpias"
        if descartados:
            msg += f"  ({descartados} descartadas)"
        log.info(msg)

    # ── Transformaciones por dataset ──────────────────────────

    def _clientes(self, df: pd.DataFrame) -> pd.DataFrame:
        antes = len(df)
        df = df.drop_duplicates(subset=["IdCliente"])
        df = df.dropna(subset=["Nombre", "Email"])
        df["Nombre"] = df["Nombre"].str.strip().str.title()
        df["Email"]  = df["Email"].str.strip().str.lower()
        mask = df["Email"].str.match(r"^[\w.\-+]+@[\w.\-]+\.\w+$", na=False)
        df = df[mask]
        df["IdCliente"] = pd.to_numeric(df["IdCliente"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["IdCliente"])

        # ── Simular campos analíticos ──────────────────────────
        n = len(df)
        df["Pais"]        = [random.choice(_PAISES) for _ in range(n)]
        df["Edad"]        = [random.randint(18, 70)  for _ in range(n)]
        # Distribución realista: 60% Regular, 30% Premium, 10% VIP
        df["TipoCliente"] = random.choices(
            _TIPOS, weights=[60, 30, 10], k=n)

        self._reporte("Clientes:", antes, len(df))
        return df

    def _productos(self, df: pd.DataFrame) -> pd.DataFrame:
        antes = len(df)
        df = df.drop_duplicates(subset=["IdProducto"])
        df = df.dropna(subset=["Nombre"])
        df["Nombre"] = df["Nombre"].str.strip().str.title()
        col_cat = next(c for c in df.columns if "ategor" in c)
        df["Categoria"] = df[col_cat].str.strip().str.title().fillna("Sin Categoría")
        df["IdProducto"] = pd.to_numeric(df["IdProducto"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["IdProducto"])
        self._reporte("Productos:", antes, len(df))
        return df

    def _fuentes(self, df: pd.DataFrame) -> pd.DataFrame:
        antes = len(df)
        df = df.drop_duplicates(subset=["IdFuente"])
        df = df.dropna(subset=["TipoFuente", "FechaCarga"])
        df["TipoFuente"]   = df["TipoFuente"].str.strip().str.title()
        df["FechaCarga"]   = self._a_fecha(df["FechaCarga"])
        df["IdFuente_num"] = pd.to_numeric(
            self._quitar_prefijo(df["IdFuente"].astype(str)), errors="coerce"
        ).astype("Int64")
        df = df.dropna(subset=["FechaCarga"])
        self._reporte("Fuentes:", antes, len(df))
        return df

    def _survey(self, df: pd.DataFrame) -> pd.DataFrame:
        antes = len(df)
        df = df.drop_duplicates(subset=["IdOpinion"])
        df = df.dropna(subset=["IdCliente", "IdProducto", "Fecha", "PuntajeSatisfacción"])
        df["IdCliente"]  = pd.to_numeric(df["IdCliente"],  errors="coerce").astype("Int64")
        df["IdProducto"] = pd.to_numeric(df["IdProducto"], errors="coerce").astype("Int64")
        df["Fecha"]      = self._a_fecha(df["Fecha"])
        df["Puntaje"]    = pd.to_numeric(df["PuntajeSatisfacción"], errors="coerce").clip(1, 5).astype("Int64")
        df["Comentario"] = df["Comentario"].str.strip().fillna("Sin comentario")
        col_clas = next(c for c in df.columns if "lasif" in c)
        df["Clasificacion"] = df[col_clas].str.strip().str.title().fillna("Neutra")
        df["FuenteNombre"]  = df["Fuente"].str.strip() if "Fuente" in df.columns else "EncuestaInterna"
        df = df.dropna(subset=["IdCliente", "IdProducto", "Fecha", "Puntaje"])
        self._reporte("Survey:", antes, len(df))
        return df

    def _social(self, df: pd.DataFrame) -> pd.DataFrame:
        antes = len(df)
        df = df.drop_duplicates(subset=["IdComment"])
        df["IdCliente_num"]  = pd.to_numeric(self._quitar_prefijo(df["IdCliente"].fillna("")),  errors="coerce")
        df["IdProducto_num"] = pd.to_numeric(self._quitar_prefijo(df["IdProducto"].fillna("")), errors="coerce")
        df = df.dropna(subset=["IdCliente_num", "IdProducto_num", "Fecha"])
        df["IdCliente_num"]  = df["IdCliente_num"].astype("Int64")
        df["IdProducto_num"] = df["IdProducto_num"].astype("Int64")
        df["Fecha"]      = self._a_fecha(df["Fecha"])
        df["Fuente"]     = df["Fuente"].str.strip().str.title().fillna("Desconocida")
        df["Comentario"] = df["Comentario"].str.strip().fillna("Sin comentario")
        df = df.dropna(subset=["Fecha"])
        self._reporte("SocialComments:", antes, len(df))
        return df

    def _webreviews(self, df: pd.DataFrame) -> pd.DataFrame:
        antes = len(df)
        df = df.drop_duplicates(subset=["IdReview"])
        df["IdCliente_num"]  = pd.to_numeric(self._quitar_prefijo(df["IdCliente"].fillna("")),  errors="coerce")
        df["IdProducto_num"] = pd.to_numeric(self._quitar_prefijo(df["IdProducto"].fillna("")), errors="coerce")
        df = df.dropna(subset=["IdCliente_num", "IdProducto_num", "Fecha", "Rating"])
        df["IdCliente_num"]  = df["IdCliente_num"].astype("Int64")
        df["IdProducto_num"] = df["IdProducto_num"].astype("Int64")
        df["Fecha"]      = self._a_fecha(df["Fecha"])
        df["Puntaje"]    = pd.to_numeric(df["Rating"], errors="coerce").clip(1, 5).astype("Int64")
        df["Comentario"] = df["Comentario"].str.strip().fillna("Sin comentario")
        df = df.dropna(subset=["Fecha", "Puntaje"])
        self._reporte("WebReviews:", antes, len(df))
        return df
