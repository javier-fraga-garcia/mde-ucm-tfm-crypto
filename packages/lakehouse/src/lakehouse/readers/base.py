from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame


class Reader(ABC):
    """Interfaz para lectores de datos dentro del lakehouse."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def read(self) -> DataFrame:
        """Lee los datos de la fuente correspondiente.

        Returns:
            DataFrame con los datos leídos.
        """
        ...
