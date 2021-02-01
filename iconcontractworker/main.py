"""
A module that creates a worker to parse ICON contract events
"""

from json import dumps
from random import randint
from socket import gethostname
from threading import Lock, Thread

import psycopg2
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.serialization import MessageField, SerializationContext
from consumers import log_consume_loop, registration_consume_loop
from pydantic import BaseSettings, Field

from iconcontractworker.bootstrap import init_state
from iconcontractworker.schema import get_logs_schema, get_registrations_schema


class Settings(BaseSettings):
    kafka_server: str = Field(..., env="contract_worker_kafka_server")
    consumer_group: str = Field("contract_worker", env="contract_worker_consumer_group")
    schema_server: str = Field(..., env="contract_worker_schema_server")
    kafka_compression: str = Field("gzip", env="contract_worker_kafka_compression")
    kafka_min_commit_count: int = Field(
        10, env="contract_worker_kafka_min_commit_count"
    )
    registrations_topic: str = Field(
        "registrations", env="contract_worker_registrations_topic"
    )
    logs_topic: str = Field("logs", env="contract_worker_logs_topic")
    output_topic: str = Field("contract_outputs", env="contract_worker_output_topic")
    db_server: str = Field(..., env="contract_worker_db_server")
    db_port: int = Field(5432, env="contract_worker_db_port")
    db_user: str = Field(..., env="contract_worker_db_user")
    db_password: str = Field(..., env="contract_worker_db_password")
    db_database: str = Field("postgres", env="contract_worker_db_database")


s = Settings()

# Kafka objects
# Producer

output_producer = Producer(
    {
        "bootstrap.servers": s.kafka_server,
        "compression.codec": s.kafka_compression,
    }
)

# Consumers (x2)

logs_consumer = Consumer(
    {
        "bootstrap.servers": s.kafka_server,
        "compression.codec": s.kafka_compression,
        "group.id": s.consumer_group,
    }
)

registrations_consumer = Consumer(
    {
        "bootstrap.servers": s.kafka_server,
        "compression.codec": s.kafka_compression,
        "group.id": gethostname() + str(randint(0, 999)),
    }
)

# Schema Registry client

schema_client = SchemaRegistryClient({"url": s.schema_server})

# Serializers

logs_value_serializer = JSONSerializer(
    dumps(get_logs_schema(s.logs_topic)),
    schema_client,
    conf={"auto.register.schemas": False},
)

# Deserializers

logs_value_deserializer = JSONDeserializer(dumps(get_logs_schema(s.logs_topic)))

registration_value_deserializer = JSONDeserializer(
    dumps(get_registrations_schema(s.registrations_topic))
)

# Message contexts

registration_value_context = SerializationContext(
    s.registrations_topic, MessageField.VALUE
)

logs_value_context = SerializationContext(s.logs_topic, MessageField.VALUE)

# Postgres connection objects

con = psycopg2.connect(
    database=s.db_database,
    user=s.db_user,
    password=s.db_password,
    host=s.db_server,
    port=s.db_port,
)

# Init the registration state table

registration_state_table = init_state(con)
registration_state_lock = Lock()

# Create & spawn the registration consumption thread

registration_thread = Thread(
    target=registration_consume_loop,
    args=(
        registrations_consumer,
        s.registrations_topic,
        s.kafka_min_commit_count,
        registration_value_deserializer,
        registration_value_context,
        registration_state_table,
        registration_state_lock,
    ),
)

registration_thread.start()

# Create & spawn the log consumption thread

logs_thread = Thread(
    target=log_consume_loop,
    args=(
        logs_consumer,
        s.logs_topic,
        s.kafka_min_commit_count,
        logs_value_deserializer,
        logs_value_context,
        output_producer,
        s.output_topic,
        registration_state_table,
        registration_state_lock,
    ),
)

logs_thread.start()
