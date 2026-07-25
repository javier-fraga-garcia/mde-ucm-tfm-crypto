from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from lakehouse.readers import Reader
from lakehouse.writers import Writer


class Ingestor(ABC):
    """Orquestador del flujo read -> transform -> write de una capa del lakehouse."""

    def __init__(self, reader: Reader, writer: Writer):
        self.reader = reader
        self.writer = writer

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Aplica la lógica de transformación propia de la capa.

        Args:
            df: DataFrame leído por el reader de la capa.

        Returns:
            DataFrame transformado, listo para ser escrito por el writer.
        """
        ...

    def run(self) -> None:
        """Ejecuta el ciclo completo de la capa: lee, transforma y escribe."""
        df = self.reader.read()
        df = self.transform(df)
        self.writer.write(df)
