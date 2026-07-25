from pyspark.sql import DataFrame

from lakehouse.ingestors import Ingestor


class BronzeIngestor(Ingestor):
    """Ingestor de la capa Bronze: persiste los eventos de Kafka sin transformarlos."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Devuelve el DataFrame sin modificar, fiel al envelope original.

        Args:
            df: DataFrame leído del reader de Bronze.

        Returns:
            El mismo DataFrame recibido, sin cambios.
        """
        return df
