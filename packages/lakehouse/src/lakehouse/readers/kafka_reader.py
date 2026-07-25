from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json

from lakehouse.readers import Reader, KAFKA_ENVELOPE_SPARK_SCHEMA


class KafkaReader(Reader):
    """Lee eventos del topic de Kafka y los parsea según el contrato de KafkaEnvelope."""

    def __init__(self, spark: SparkSession, bootstrap_servers: str, topic: str):
        super().__init__(spark)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    def read(self) -> DataFrame:
        """Lee el topic de Kafka en streaming y parsea el envelope.

        Returns:
            DataFrame con los campos del envelope desplegados, más
            partition, offset y kafka_timestamp para trazabilidad.
        """
        raw_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        json_df = raw_df.select(
            col("value").cast("string").alias("json_value"),
            col("partition"),
            col("offset"),
            col("timestamp"),
        )

        result_df = json_df.select(
            from_json(col("json_value"), KAFKA_ENVELOPE_SPARK_SCHEMA).alias("envelope"),
            col("partition"),
            col("offset"),
            col("timestamp"),
        ).select(
            "envelope.*",
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )

        return result_df
