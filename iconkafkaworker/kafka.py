from json import dumps
from random import randint
from socket import gethostname

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.serialization import MessageField, SerializationContext

from iconkafkaworker.schema import (
    get_logs_schema,
    get_registrations_schema,
    get_transactions_schema,
)
from iconkafkaworker.settings import settings

# Kafka objects
# Producer

output_producer = Producer(
    {
        "bootstrap.servers": settings.kafka_server,
        "compression.codec": settings.kafka_compression,
    }
)

# Consumers

event_consumer = Consumer(
    {
        "bootstrap.servers": settings.kafka_server,
        "compression.codec": settings.kafka_compression,
        "group.id": settings.consumer_group + "-" + str(settings.processing_mode),
    }
)

registrations_consumer = Consumer(
    {
        "bootstrap.servers": settings.kafka_server,
        "compression.codec": settings.kafka_compression,
        "group.id": gethostname() + str(randint(0, 999)),
    }
)

# Schema Registry client

schema_client = SchemaRegistryClient({"url": settings.schema_server})

# Serializers

logs_value_serializer = JSONSerializer(
    dumps(get_logs_schema(settings.logs_topic)),
    schema_client,
    conf={"auto.register.schemas": False},
)

transactions_value_serializer = JSONSerializer(
    dumps(get_transactions_schema(settings.transactions_topic)),
    schema_client,
    conf={"auto.register.schemas": False},
)

# Deserializers

logs_value_deserializer = JSONDeserializer(dumps(get_logs_schema(settings.logs_topic)))

transactions_value_deserializer = JSONDeserializer(
    dumps(get_transactions_schema(settings.transactions_topic))
)

registration_value_deserializer = JSONDeserializer(
    dumps(get_registrations_schema(settings.registrations_topic))
)

# Message contexts

registration_value_context = SerializationContext(
    settings.registrations_topic, MessageField.VALUE
)

logs_value_context = SerializationContext(settings.logs_topic, MessageField.VALUE)

transactions_value_context = SerializationContext(
    settings.transactions_topic, MessageField.VALUE
)
