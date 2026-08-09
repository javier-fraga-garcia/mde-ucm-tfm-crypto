"""Fixtures compartidos para los tests de lakehouse."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("lakehouse-tests")
        .master("local[1]")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )
    yield session
    session.stop()
