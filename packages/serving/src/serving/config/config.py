from pydantic import Field

from shared.config import BaseSettings


class ServingSettings(BaseSettings):
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str

    gold_volatility_table_path: str = Field(
        validation_alias="SERVING_GOLD_VOLATILITY_TABLE_PATH"
    )
    gold_spread_table_path: str = Field(
        validation_alias="SERVING_GOLD_SPREAD_TABLE_PATH"
    )
    gold_liquidity_table_path: str = Field(
        validation_alias="SERVING_GOLD_LIQUIDITY_TABLE_PATH"
    )

    timescale_host: str
    timescale_port: int
    timescale_database: str
    timescale_user: str
    timescale_password: str

    sync_interval_seconds: int
