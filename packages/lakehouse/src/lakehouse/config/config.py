from shared.config import BaseSettings


class LakehouseSettings(BaseSettings):
    kafka_bootstrap_servers: str
    kafka_topic: str

    bronze_table_path: str
    bronze_checkpoint_path: str
    bronze_trigger_interval: str

    silver_table_path: str
    silver_checkpoint_path: str
    silver_trigger_interval: str

    gold_table_path: str
    gold_checkpoint_path: str
    gold_trigger_interval: str

    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
