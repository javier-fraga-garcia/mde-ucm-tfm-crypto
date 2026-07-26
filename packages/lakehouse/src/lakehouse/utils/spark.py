from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_or_create_spark_session(
    app_name: str,
    s3_endpoint_url: str,
    s3_access_key: str,
    s3_secret_key: str,
) -> SparkSession:
    """
    Obtiene la SparkSession activa si existe, o crea una nueva con
    soporte para Delta Lake, Kafka y almacenamiento S3A (Floci) en caso contrario.

    Args:
        app_name: Nombre de la aplicación Spark.
        s3_endpoint_url: URL del endpoint S3 (Floci).
        s3_access_key: Access key para el endpoint S3.
        s3_secret_key: Secret key para el endpoint S3.

    Returns:
        SparkSession activa con Delta Lake, Kafka y S3A configurados.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )
    extra_packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    ]
    return configure_spark_with_delta_pip(
        builder, extra_packages=extra_packages
    ).getOrCreate()
