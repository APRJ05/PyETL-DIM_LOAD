"""
etl/loader_star.py — Paso 4: Carga del modelo analítico (OLAP)
Lee datos procesados de OpClientes (OLTP) y los carga en OpClientes_DW (OLAP).
Debe ejecutarse DESPUÉS de loader.py.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.interfaces import IStarLoader
from utils.db import get_connection, get_dw_connection
from utils.logger import get_logger

log = get_logger(__name__)


class LoaderStar(IStarLoader):
    """
    Pipeline OLTP → OLAP:
        Lee de OpClientes:    Clientes, Producto, Categorias, Fuentes, Rating,
                              Survey, SocialComments, WebReviews
        Escribe en OpClientes_DW: DIM_Fecha, DIM_Cliente, DIM_Producto,
                                   DIM_Fuente, DIM_Rating, FACT_Opiniones
    """

    def __init__(self):
        self.oltp = None   # conexión OpClientes
        self.olap = None   # conexión OpClientes_DW
        self.cur_oltp = None
        self.cur_olap = None

    # ── Entry point ───────────────────────────────────────────

    def run(self):
        log.info("=" * 55)
        log.info("PASO 4 — CARGA MODELO ANALÍTICO (OpClientes_DW)")
        log.info("=" * 55)

        self.oltp     = get_connection()
        self.olap     = get_dw_connection()
        self.cur_oltp = self.oltp.cursor()
        self.cur_olap = self.olap.cursor()

        try:
            self._cargar_dim_fecha()
            self._cargar_dim_cliente()
            self._cargar_dim_producto()
            self._cargar_dim_fuente()
            self._cargar_dim_rating()
            self._cargar_fact_opiniones()

            log.info("=" * 55)
            log.info("✅  OpClientes_DW CARGADO EXITOSAMENTE")
            log.info("=" * 55)

        except Exception as e:
            self.olap.rollback()
            log.error(f"🔴 Error en OLAP — rollback ejecutado. Detalle: {e}")
            raise
        finally:
            self.cur_oltp.close()
            self.cur_olap.close()
            self.oltp.close()
            self.olap.close()

    # ── Helpers ───────────────────────────────────────────────

    def _commit(self):
        self.olap.commit()

    def _count(self, tabla: str) -> int:
        self.cur_olap.execute(f"SELECT COUNT(*) FROM {tabla}")
        return self.cur_olap.fetchone()[0]

    # ── DIM_Fecha ─────────────────────────────────────────────

    def _cargar_dim_fecha(self):
        self.cur_olap.execute("DELETE FROM DIM_Fecha")
        self.cur_olap.execute("""
            INSERT INTO DIM_Fecha
                (ID_Fecha, Fecha, Dia, Mes, NombreMes, Trimestre, Anio, Semana, DiaSemana)
            SELECT DISTINCT
                CAST(FORMAT(f.Fecha, 'yyyyMMdd') AS INT),
                f.Fecha,
                DAY(f.Fecha),
                MONTH(f.Fecha),
                DATENAME(MONTH,   f.Fecha),
                DATEPART(QUARTER, f.Fecha),
                YEAR(f.Fecha),
                DATEPART(WEEK,    f.Fecha),
                DATENAME(WEEKDAY, f.Fecha)
            FROM (
                SELECT Fecha FROM OpClientes.dbo.Survey
                UNION
                SELECT Fecha FROM OpClientes.dbo.SocialComments
                UNION
                SELECT Fecha FROM OpClientes.dbo.WebReviews
            ) AS f
        """)
        self._commit()
        log.info(f"  ✓ {'DIM_Fecha':<22} {self._count('DIM_Fecha')} registros")

    # ── DIM_Cliente ───────────────────────────────────────────

    def _cargar_dim_cliente(self):
        self.cur_olap.execute("DELETE FROM DIM_Cliente")
        self.cur_olap.execute("""
            INSERT INTO DIM_Cliente (ID_Cliente, Nombre, Email, Pais, Edad, TipoCliente)
            SELECT ID_Cliente, Nombre, Email, Pais, Edad, TipoCliente
            FROM OpClientes.dbo.Clientes
        """)
        self._commit()
        log.info(f"  ✓ {'DIM_Cliente':<22} {self._count('DIM_Cliente')} registros")

    # ── DIM_Producto ──────────────────────────────────────────

    def _cargar_dim_producto(self):
        self.cur_olap.execute("DELETE FROM DIM_Producto")
        self.cur_olap.execute("""
            INSERT INTO DIM_Producto (ID_Producto, Nombre, Categoria)
            SELECT P.ID_Producto, P.Nombre, C.TipoCategoria
            FROM OpClientes.dbo.Producto P
            JOIN OpClientes.dbo.Categorias C ON C.ID_Categoria = P.ID_Categoria
        """)
        self._commit()
        log.info(f"  ✓ {'DIM_Producto':<22} {self._count('DIM_Producto')} registros")

    # ── DIM_Fuente ────────────────────────────────────────────

    def _cargar_dim_fuente(self):
        self.cur_olap.execute("DELETE FROM DIM_Fuente")
        self.cur_olap.execute("""
            INSERT INTO DIM_Fuente (ID_Fuente, TipoFuente)
            SELECT ID_Fuente, TipoFuente
            FROM OpClientes.dbo.Fuentes
        """)
        self._commit()
        log.info(f"  ✓ {'DIM_Fuente':<22} {self._count('DIM_Fuente')} registros")

    # ── DIM_Rating ────────────────────────────────────────────

    def _cargar_dim_rating(self):
        self.cur_olap.execute("DELETE FROM DIM_Rating")
        self.cur_olap.execute("""
            INSERT INTO DIM_Rating (Puntaje, Clasificacion)
            SELECT Puntaje, Clasificación
            FROM OpClientes.dbo.Rating
        """)
        self._commit()
        log.info(f"  ✓ {'DIM_Rating':<22} {self._count('DIM_Rating')} registros")

    # ── FACT_Opiniones ────────────────────────────────────────

    def _cargar_fact_opiniones(self):
        self.cur_olap.execute("DELETE FROM FACT_Opiniones")

        # Survey
        self.cur_olap.execute("""
            INSERT INTO FACT_Opiniones
                (ID_Fecha, ID_Cliente, ID_Producto, ID_Fuente, Puntaje, Canal)
            SELECT
                CAST(FORMAT(Fecha, 'yyyyMMdd') AS INT),
                ID_Cliente, ID_Producto, ID_Fuente, Puntaje, 'Survey'
            FROM OpClientes.dbo.Survey
        """)

        # WebReviews
        self.cur_olap.execute("""
            INSERT INTO FACT_Opiniones
                (ID_Fecha, ID_Cliente, ID_Producto, ID_Fuente, Puntaje, Canal)
            SELECT
                CAST(FORMAT(Fecha, 'yyyyMMdd') AS INT),
                ID_Cliente, ID_Producto, ID_Fuente, Puntaje, 'WebReview'
            FROM OpClientes.dbo.WebReviews
        """)

        # SocialComments — sin puntaje, se asigna 3 (Neutro)
        self.cur_olap.execute("""
            INSERT INTO FACT_Opiniones
                (ID_Fecha, ID_Cliente, ID_Producto, ID_Fuente, Puntaje, Canal)
            SELECT
                CAST(FORMAT(Fecha, 'yyyyMMdd') AS INT),
                ID_Cliente, ID_Producto, ID_Fuente, 3, 'Social'
            FROM OpClientes.dbo.SocialComments
        """)

        self._commit()

        # Reporte por canal
        self.cur_olap.execute("SELECT Canal, COUNT(*) FROM FACT_Opiniones GROUP BY Canal")
        total = 0
        for canal, cnt in self.cur_olap.fetchall():
            log.info(f"  ✓ FACT_Opiniones [{canal:<12}] {cnt} registros")
            total += cnt
        log.info(f"  ✓ {'FACT_Opiniones TOTAL':<22} {total} registros")
