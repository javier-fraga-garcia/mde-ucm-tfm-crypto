from shared.config import BaseSettings


class ServingSettings(BaseSettings):
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str

    gold_volatility_table_path: str
    gold_spread_table_path: str
    gold_liquidity_table_path: str

    timescale_host: str
    timescale_port: int
    timescale_database: str
    timescale_user: str
    timescale_password: str

    sync_interval_seconds: int
