from shared.config import BaseSettings


class LakehouseSettings(BaseSettings):
    kafka_bootstrap_servers: str
    kafka_topic: str

    bronze_table_path: str
    bronze_checkpoint_path: str
    bronze_trigger_interval: str

    silver_agg_trade_table_path: str
    silver_agg_trade_checkpoint_path: str
    silver_agg_trade_trigger_interval: str

    silver_book_ticker_table_path: str
    silver_book_ticker_checkpoint_path: str
    silver_book_ticker_trigger_interval: str

    silver_depth10_table_path: str
    silver_depth10_checkpoint_path: str
    silver_depth10_trigger_interval: str

    gold_table_path: str
    gold_checkpoint_path: str
    gold_trigger_interval: str

    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
