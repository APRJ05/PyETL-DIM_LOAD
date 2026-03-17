from abc import ABC, abstractmethod
import pandas as pd


class IExtractor(ABC):
    """
    Contrato para cualquier extractor de datos.
    Implementaciones: CsvExtractor, DatabaseExtractor, ApiExtractor.
    """

    @abstractmethod
    def extract(self) -> dict[str, pd.DataFrame]:
        """
        Extrae datos de la fuente y los retorna como DataFrames.
        Returns:
            dict con nombre de dataset → DataFrame crudo
        """
        pass

    @abstractmethod
    def validate(self, data: dict[str, pd.DataFrame]) -> bool:
        """
        Valida que los datos extraídos sean estructuralmente correctos.
        Returns:
            True si la validación pasa, False si hay problemas críticos.
        """
        pass


class ITransformer(ABC):
    """
    Contrato para el componente de transformación y limpieza.
    """

    @abstractmethod
    def run(self, raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """
        Aplica limpieza, normalización y enriquecimiento.
        Args:
            raw: DataFrames crudos del extractor
        Returns:
            DataFrames limpios listos para carga
        """
        pass


class ILoader(ABC):
    """
    Contrato para cualquier loader de datos.
    Implementaciones: Loader (OLTP), LoaderStar (OLAP).
    """

    @abstractmethod
    def run(self, datos: dict[str, pd.DataFrame]) -> None:
        """
        Carga los DataFrames transformados en el destino.
        Args:
            datos: DataFrames limpios del transformer
        """
        pass


class IStarLoader(ABC):
    """
    Contrato específico para el loader del modelo analítico.
    No recibe DataFrames — lee directamente del OLTP ya cargado.
    """

    @abstractmethod
    def run(self) -> None:
        """
        Lee de la BD transaccional y carga el modelo estrella analítico.
        """
        pass
