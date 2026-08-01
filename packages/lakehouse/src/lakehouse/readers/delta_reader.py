from pyspark.sql import SparkSession, DataFrame

from lakehouse.readers.base import Reader


class DeltaReader(Reader):
    """Lee una tabla Delta en modo streaming."""

    def __init__(self, spark: SparkSession, table_path: str):
        super().__init__(spark)
        self.table_path = table_path

    def read(self) -> DataFrame:
        """Lee la tabla Delta configurada en modo streaming.

        Returns:
            DataFrame de streaming con el contenido de la tabla.
        """
        return self.spark.readStream.format("delta").load(self.table_path)
