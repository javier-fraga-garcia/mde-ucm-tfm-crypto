from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame


class Writer(ABC):
    """Interfaz para escritores de datos dentro del lakehouse."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """Escribe el DataFrame en el destino correspondiente.

        Args:
            df: DataFrame con los datos a escribir.
        """
        ...
