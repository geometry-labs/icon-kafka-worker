#  Copyright 2021 Geometry Labs, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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
        "bootstrap.servers": settings.KAFKA_SERVER,
        "compression.codec": settings.KAFKA_COMPRESSION,
    }
)

# Consumers

event_consumer = Consumer(
    {
        "bootstrap.servers": settings.KAFKA_SERVER,
        "compression.codec": settings.KAFKA_COMPRESSION,
        "group.id": settings.CONSUMER_GROUP + "-" + str(settings.PROCESSING_MODE),
    }
)

registrations_consumer = Consumer(
    {
        "bootstrap.servers": settings.KAFKA_SERVER,
        "compression.codec": settings.KAFKA_COMPRESSION,
        "group.id": gethostname() + str(randint(0, 999)),
    }
)

# Schema Registry client
if settings.SCHEMA_SERVER:

    schema_client = SchemaRegistryClient({"url": settings.SCHEMA_SERVER})

    # Serializers

    logs_value_serializer = JSONSerializer(
        dumps(get_logs_schema(settings.LOGS_TOPIC)),
        schema_client,
        conf={"auto.register.schemas": False},
    )

    transactions_value_serializer = JSONSerializer(
        dumps(get_transactions_schema(settings.TRANSACTIONS_TOPIC)),
        schema_client,
        conf={"auto.register.schemas": False},
    )

    # Deserializers

    logs_value_deserializer = JSONDeserializer(
        dumps(get_logs_schema(settings.LOGS_TOPIC))
    )

    transactions_value_deserializer = JSONDeserializer(
        dumps(get_transactions_schema(settings.TRANSACTIONS_TOPIC))
    )

    registration_value_deserializer = JSONDeserializer(
        dumps(get_registrations_schema(settings.REGISTRATIONS_TOPIC))
    )
else:
    schema_client = None
    logs_value_serializer = None
    transactions_value_serializer = None
    logs_value_deserializer = None
    transactions_value_deserializer = None
    registration_value_deserializer = None

# Message contexts

registration_value_context = SerializationContext(
    settings.REGISTRATIONS_TOPIC, MessageField.VALUE
)

logs_value_context = SerializationContext(settings.LOGS_TOPIC, MessageField.VALUE)

transactions_value_context = SerializationContext(
    settings.TRANSACTIONS_TOPIC, MessageField.VALUE
)
