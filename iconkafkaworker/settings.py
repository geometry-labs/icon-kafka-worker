from enum import Enum

from pydantic import BaseSettings, Field


class Mode(Enum):
    CONTRACT = "contract"
    TRANSACTION = "transaction"


class Settings(BaseSettings):
    kafka_server: str = Field(..., env="contract_worker_kafka_server")
    consumer_group: str = Field("contract_worker", env="contract_worker_consumer_group")
    schema_server: str = Field(..., env="contract_worker_schema_server")
    kafka_compression: str = Field("gzip", env="contract_worker_kafka_compression")
    kafka_min_commit_count: int = Field(
        10, env="contract_worker_kafka_min_commit_count"
    )
    registrations_topic: str = Field(
        "event_registrations", env="contract_worker_registrations_topic"
    )
    broadcaster_events_topic: str = Field(
        "broadcaster_events", env="contract_worker_broadcaster_events_topic"
    )
    broadcaster_events_table: str = Field(
        "broadcaster_registrations", env="contract_worker_broadcaster_events_table"
    )
    logs_topic: str = Field("logs", env="contract_worker_logs_topic")
    transactions_topic: str = Field(
        "transactions", env="contract_worker_transactions_topic"
    )
    output_topic: str = Field(..., env="contract_worker_output_topic")
    db_server: str = Field(..., env="contract_worker_db_server")
    db_port: int = Field(5432, env="contract_worker_db_port")
    db_user: str = Field(..., env="contract_worker_db_user")
    db_password: str = Field(..., env="contract_worker_db_password")
    db_database: str = Field("postgres", env="contract_worker_db_database")
    processing_mode: Mode = Field(Mode.CONTRACT, env="contract_worker_processing_mode")


settings = Settings()
