from pyspark.sql import SparkSession, DataFrame

from lakehouse.writers import Writer


class DeltaWriter(Writer):
    """Escribe un DataFrame de streaming en una tabla Delta."""

    def __init__(
        self,
        spark: SparkSession,
        table_path: str,
        checkpoint_path: str,
        trigger_interval: str = "30 seconds",
        partition_by: str | None = None,
    ):
        super().__init__(spark)
        self.table_path = table_path
        self.checkpoint_path = checkpoint_path
        self.trigger_interval = trigger_interval
        self.partition_by = partition_by

    def write(self, df: DataFrame) -> None:
        """Inicia la escritura en streaming a la tabla Delta y bloquea hasta que termine.

        Args:
            df: DataFrame de streaming a escribir.
        """
        writer = (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_path)
            .trigger(processingTime=self.trigger_interval)
        )

        if self.partition_by is not None:
            writer = writer.partitionBy(self.partition_by)

        query = writer.start(path=self.table_path)
        query.awaitTermination()
