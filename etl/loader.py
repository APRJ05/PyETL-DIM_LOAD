"""
etl/loader.py — Paso 3: Carga de datos en SQL Server
Respeta el orden de inserción según las FK del modelo.
"""

import pandas as pd
from etl.interfaces import ILoader
from utils.db import get_connection
from utils.logger import get_logger

log = get_logger(__name__)


class Loader(ILoader):
    """
    Carga los DataFrames transformados en las tablas de SQL Server.
    Orden de inserción:
        1. Categorias  2. Fuentes    3. Rating
        4. Clientes    5. Producto   6. Comentarios
        7. Survey      8. SocialComments  9. WebReviews
    """

    def __init__(self):
        self.conn   = None
        self.cursor = None
        # Mapas nombre/clave → ID real en BD (se construyen durante la carga)
        self.cat_map     : dict = {}
        self.fuente_map  : dict = {}
        self.cli_csv_map : dict = {}   # IdCliente_csv (int) → ID_bd
        self.prod_csv_map: dict = {}   # IdProducto_csv (int) → ID_bd
        self.com_map     : dict = {}   # texto → ID_bd

    # ── Entry point ───────────────────────────────────────────

    def run(self, datos: dict[str, pd.DataFrame]):
        log.info("=" * 55)
        log.info("PASO 3 — CARGA EN BASE DE DATOS")
        log.info("=" * 55)

        self.conn   = get_connection()
        self.cursor = self.conn.cursor()

        try:
            self._cargar_categorias(datos["productos"])
            self._cargar_fuentes(datos["fuentes"])
            self._cargar_rating()
            self._cargar_clientes(datos["clientes"])
            self._cargar_productos(datos["productos"])
            self._cargar_comentarios(datos)
            self._cargar_survey(datos["survey"])
            self._cargar_social(datos["social"])
            self._cargar_webreviews(datos["webreviews"])

            log.info("=" * 55)
            log.info("✅  PIPELINE COMPLETADO EXITOSAMENTE")
            log.info("=" * 55)

        except Exception as e:
            self.conn.rollback()
            log.error(f"🔴 Error — rollback ejecutado. Detalle: {e}")
            raise
        finally:
            self.cursor.close()
            self.conn.close()

    # ── Helpers ───────────────────────────────────────────────

    def _commit(self):
        self.conn.commit()

    @staticmethod
    def _mapear_id(id_csv: int, id_map: dict) -> int | None:
        """
        Resuelve un ID del CSV al ID real en BD.
        Si el ID no existe directamente (datos de prueba fuera de rango),
        aplica módulo para ciclar dentro del rango válido disponible.
        """
        if id_csv in id_map:
            return id_map[id_csv]
        keys = sorted(id_map.keys())
        if not keys:
            return None
        return id_map[keys[(id_csv - 1) % len(keys)]]

    def _exec(self, sql: str, *params):
        self.cursor.execute(sql, *params)

    def _executemany(self, sql: str, filas: list, tabla: str):
        if not filas:
            log.warning(f"  ⚠ {tabla}: sin filas para insertar.")
            return
        self.cursor.executemany(sql, filas)
        self._commit()
        log.info(f"  ✓ {tabla:<20} {len(filas)} registros insertados")

    def _refrescar_mapa(self, sql: str) -> dict:
        self.cursor.execute(sql)
        return {r[1]: r[0] for r in self.cursor.fetchall()}

    # ── Carga por tabla ───────────────────────────────────────

    def _cargar_categorias(self, df_productos: pd.DataFrame):
        cats = df_productos["Categoria"].dropna().unique()
        for cat in cats:
            self._exec(
                "IF NOT EXISTS (SELECT 1 FROM Categorias WHERE TipoCategoria=?) "
                "INSERT INTO Categorias (TipoCategoria) VALUES (?)", cat, cat)
        self._commit()
        self.cat_map = self._refrescar_mapa("SELECT ID_Categoria, TipoCategoria FROM Categorias")
        log.info(f"  ✓ {'Categorias':<20} {len(self.cat_map)} registros")

    def _cargar_fuentes(self, df_fuentes: pd.DataFrame):
        # Fuentes del CSV
        for _, r in df_fuentes.iterrows():
            self._exec(
                "IF NOT EXISTS (SELECT 1 FROM Fuentes WHERE TipoFuente=?) "
                "INSERT INTO Fuentes (TipoFuente, FechaCarga) VALUES (?,?)",
                r["TipoFuente"], r["TipoFuente"], r["FechaCarga"])
        # Fuentes adicionales usadas en Survey y SocialComments
        extras = [("EncuestaInterna", "2025-01-01"), ("Instagram", "2025-01-01"),
                  ("Twitter", "2025-01-01"), ("Facebook", "2025-01-01")]
        for tipo, fecha in extras:
            self._exec(
                "IF NOT EXISTS (SELECT 1 FROM Fuentes WHERE TipoFuente=?) "
                "INSERT INTO Fuentes (TipoFuente, FechaCarga) VALUES (?,?)",
                tipo, tipo, fecha)
        self._commit()
        self.fuente_map = self._refrescar_mapa("SELECT ID_Fuente, TipoFuente FROM Fuentes")
        log.info(f"  ✓ {'Fuentes':<20} {len(self.fuente_map)} registros")

    def _cargar_rating(self):
        labels = {1: "Muy Malo", 2: "Malo", 3: "Neutro", 4: "Bueno", 5: "Excelente"}
        for puntaje, clasif in labels.items():
            self._exec(
                "IF NOT EXISTS (SELECT 1 FROM Rating WHERE Puntaje=?) "
                "INSERT INTO Rating (Puntaje, Clasificación) VALUES (?,?)",
                puntaje, puntaje, clasif)
        self._commit()
        log.info(f"  ✓ {'Rating':<20} 5 registros (1-5)")

    def _cargar_clientes(self, df: pd.DataFrame):
        for _, r in df.iterrows():
            self._exec(
                "IF NOT EXISTS (SELECT 1 FROM Clientes WHERE Email=?) "
                "INSERT INTO Clientes (Nombre, Email, Pais, Edad, TipoCliente) "
                "VALUES (?,?,?,?,?)",
                r["Email"], r["Nombre"], r["Email"],
                r["Pais"], int(r["Edad"]), r["TipoCliente"])
        self._commit()
        cli_email_map = self._refrescar_mapa("SELECT ID_Cliente, Email FROM Clientes")
        # Construir mapa IdCSV → ID_bd
        for _, r in df.iterrows():
            id_bd = cli_email_map.get(r["Email"])
            if id_bd:
                self.cli_csv_map[int(r["IdCliente"])] = id_bd
        log.info(f"  ✓ {'Clientes':<20} {len(self.cli_csv_map)} registros")

    def _cargar_productos(self, df: pd.DataFrame):
        for _, r in df.iterrows():
            id_cat = self.cat_map.get(r["Categoria"], list(self.cat_map.values())[0])
            self._exec(
                "IF NOT EXISTS (SELECT 1 FROM Producto WHERE Nombre=?) "
                "INSERT INTO Producto (Nombre, ID_Categoria) VALUES (?,?)",
                r["Nombre"], r["Nombre"], id_cat)
        self._commit()
        prod_nombre_map = self._refrescar_mapa("SELECT ID_Producto, Nombre FROM Producto")
        for _, r in df.iterrows():
            id_bd = prod_nombre_map.get(r["Nombre"])
            if id_bd:
                self.prod_csv_map[int(r["IdProducto"])] = id_bd
        log.info(f"  ✓ {'Producto':<20} {len(self.prod_csv_map)} registros")

    def _cargar_comentarios(self, datos: dict):
        textos = pd.concat([
            datos["survey"]["Comentario"],
            datos["social"]["Comentario"],
            datos["webreviews"]["Comentario"],
        ]).dropna().str.strip().unique()
        for texto in textos:
            if texto:
                self._exec(
                    "IF NOT EXISTS (SELECT 1 FROM Comentarios WHERE Texto_Comentario=?) "
                    "INSERT INTO Comentarios (Texto_Comentario) VALUES (?)",
                    texto, texto)
        self._commit()
        self.com_map = self._refrescar_mapa("SELECT ID_Comentario, Texto_Comentario FROM Comentarios")
        log.info(f"  ✓ {'Comentarios':<20} {len(self.com_map)} registros")

    def _cargar_survey(self, df: pd.DataFrame):
        fallback_com = list(self.com_map.values())[0]
        fallback_fue = self.fuente_map.get("EncuestaInterna", list(self.fuente_map.values())[0])
        filas = []
        for _, r in df.iterrows():
            id_cli  = self._mapear_id(int(r["IdCliente"]),  self.cli_csv_map)
            id_prod = self._mapear_id(int(r["IdProducto"]), self.prod_csv_map)
            id_fue  = self.fuente_map.get(str(r.get("FuenteNombre", "")).strip(), fallback_fue)
            id_com  = self.com_map.get(r["Comentario"].strip(), fallback_com)
            if not all([id_cli, id_prod, id_fue, id_com]):
                continue
            filas.append((r["Fecha"], int(r["Puntaje"]), id_cli, id_prod, id_fue, id_com))
        self._executemany(
            "INSERT INTO Survey (Fecha, Puntaje, ID_Cliente, ID_Producto, ID_Fuente, ID_Comentario) "
            "VALUES (?,?,?,?,?,?)", filas, "Survey")

    def _cargar_social(self, df: pd.DataFrame):
        fallback_com = list(self.com_map.values())[0]
        filas = []
        for _, r in df.iterrows():
            id_cli  = self._mapear_id(int(r["IdCliente_num"]),  self.cli_csv_map)
            id_prod = self._mapear_id(int(r["IdProducto_num"]), self.prod_csv_map)
            id_fue  = self.fuente_map.get(str(r["Fuente"]).strip(), list(self.fuente_map.values())[0])
            id_com  = self.com_map.get(r["Comentario"].strip(), fallback_com)
            if not all([id_cli, id_prod, id_fue, id_com]):
                continue
            filas.append((r["Fecha"], id_cli, id_prod, id_fue, id_com))
        self._executemany(
            "INSERT INTO SocialComments (Fecha, ID_Cliente, ID_Producto, ID_Fuente, ID_Comentario) "
            "VALUES (?,?,?,?,?)", filas, "SocialComments")

    def _cargar_webreviews(self, df: pd.DataFrame):
        fallback_com = list(self.com_map.values())[0]
        id_fuente_web = self.fuente_map.get("Web", list(self.fuente_map.values())[0])
        filas = []
        for _, r in df.iterrows():
            id_cli  = self._mapear_id(int(r["IdCliente_num"]),  self.cli_csv_map)
            id_prod = self._mapear_id(int(r["IdProducto_num"]), self.prod_csv_map)
            id_com  = self.com_map.get(r["Comentario"].strip(), fallback_com)
            if not all([id_cli, id_prod, id_com]):
                continue
            filas.append((r["Fecha"], int(r["Puntaje"]), id_cli, id_prod, id_fuente_web, id_com))
        self._executemany(
            "INSERT INTO WebReviews (Fecha, Puntaje, ID_Cliente, ID_Producto, ID_Fuente, ID_Comentario) "
            "VALUES (?,?,?,?,?,?)", filas, "WebReviews")
