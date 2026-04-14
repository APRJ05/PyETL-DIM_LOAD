from abc import ABC, abstractmethod
import pandas as pd


class IExtractor(ABC):

    @abstractmethod
    def extract(self) -> dict[str, pd.DataFrame]:

        pass

    @abstractmethod
    def validate(self, data: dict[str, pd.DataFrame]) -> bool:

        pass


class ITransformer(ABC):


    @abstractmethod
    def run(self, raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:

        pass


class ILoader(ABC):


    @abstractmethod
    def run(self, datos: dict[str, pd.DataFrame]) -> None:

        pass


class IStarLoader(ABC):


    @abstractmethod
    def run(self) -> None:

        pass
