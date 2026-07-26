"""Test de coherencia entre KAFKA_ENVELOPE_SPARK_SCHEMA y KafkaEnvelope."""

from lakehouse.readers.schemas import KAFKA_ENVELOPE_SPARK_SCHEMA
from shared.schemas import KafkaEnvelope


def test_spark_schema_matches_kafka_envelope_fields():
    assert set(KAFKA_ENVELOPE_SPARK_SCHEMA.fieldNames()) == set(KafkaEnvelope.model_fields.keys())