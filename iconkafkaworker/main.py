"""
A module that creates a worker to parse ICON contract events
"""


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

import logging
from threading import Lock, Thread

import psycopg2

from iconkafkaworker.bootstrap import (
    init_log_registration_state,
    init_tx_registration_state,
)
from iconkafkaworker.consumers.log import log_consume_loop
from iconkafkaworker.consumers.registration import registration_consume_loop
from iconkafkaworker.consumers.transaction import transaction_consume_loop
from iconkafkaworker.kafka import *
from iconkafkaworker.settings import Mode, settings

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s :: %(levelname)s :: %(message)s"
)

logging.info("ICON Kafka Worker is starting up...")

# Postgres connection objects

con = psycopg2.connect(
    database=settings.POSTGRES_DATABASE,
    user=settings.POSTGRES_USER,
    password=settings.POSTGRES_PASSWORD,
    host=settings.POSTGRES_SERVER,
    port=settings.POSTGRES_PORT,
)

# Init the registration state table

if settings.PROCESSING_MODE == Mode.CONTRACT:
    logging.info("Worker is in CONTRACT MODE")
    (
        registration_state_table,
        broadcaster_events_table,
        reverse_search_dict,
    ) = init_log_registration_state(con)
    logging.info("Local states are initialized")
    registration_state_lock = Lock()

elif settings.PROCESSING_MODE == Mode.TRANSACTION:
    logging.info("Worker is in TRANSACTION MODE")
    (
        registration_state_table,
        broadcaster_events_table,
        reverse_search_dict,
    ) = init_tx_registration_state(con)
    logging.info("Local states are initialized")
    registration_state_lock = Lock()

else:
    raise ValueError(
        "The provided processing mode, {}, is not valid.".format(
            settings.PROCESSING_MODE
        )
    )

# Create & spawn the registration consumption thread

logging.info("Spawning registration thread...")

registration_thread = Thread(
    target=registration_consume_loop,
    args=(
        settings.PROCESSING_MODE,
        [settings.REGISTRATIONS_TOPIC, settings.BROADCASTER_EVENTS_TOPIC],
        settings.KAFKA_MIN_COMMIT_COUNT,
        registration_value_deserializer,
        registration_value_context,
        registration_state_table,
        broadcaster_events_table,
        reverse_search_dict,
        registration_state_lock,
    ),
)

registration_thread.start()

# Create & spawn the log consumption thread
if settings.PROCESSING_MODE == Mode.CONTRACT:
    logging.info("Spawning processing thread...")

    logs_thread = Thread(
        target=log_consume_loop,
        args=(
            settings.LOGS_TOPIC,
            settings.KAFKA_MIN_COMMIT_COUNT,
            logs_value_deserializer,
            logs_value_context,
            settings.OUTPUT_TOPIC,
            registration_state_table,
            broadcaster_events_table,
            registration_state_lock,
        ),
    )

    logs_thread.start()

if settings.PROCESSING_MODE == Mode.TRANSACTION:
    logging.info("Spawning processing thread...")
    tx_thread = Thread(
        target=transaction_consume_loop,
        args=(
            settings.TRANSACTIONS_TOPIC,
            settings.KAFKA_MIN_COMMIT_COUNT,
            transactions_value_deserializer,
            transactions_value_context,
            settings.OUTPUT_TOPIC,
            registration_state_table,
            broadcaster_events_table,
            registration_state_lock,
        ),
    )

    tx_thread.start()
