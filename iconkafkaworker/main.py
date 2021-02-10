"""
A module that creates a worker to parse ICON contract events
"""


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

# Postgres connection objects

con = psycopg2.connect(
    database=settings.db_database,
    user=settings.db_user,
    password=settings.db_password,
    host=settings.db_server,
    port=settings.db_port,
)

# Init the registration state table

if settings.processing_mode == Mode.CONTRACT:
    (
        registration_state_table,
        broadcaster_events_table,
        reverse_search_dict,
    ) = init_log_registration_state(con)
    registration_state_lock = Lock()

elif settings.processing_mode == Mode.TRANSACTION:
    (
        registration_state_table,
        broadcaster_events_table,
        reverse_search_dict,
    ) = init_tx_registration_state(con)
    registration_state_lock = Lock()

else:
    raise ValueError(
        "The provided processing mode, {}, is not valid.".format(
            settings.processing_mode
        )
    )

# Create & spawn the registration consumption thread

registration_thread = Thread(
    target=registration_consume_loop,
    args=(
        settings.processing_mode,
        registrations_consumer,
        [settings.registrations_topic, settings.broadcaster_events_topic],
        settings.kafka_min_commit_count,
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
if settings.processing_mode == Mode.CONTRACT:
    logs_thread = Thread(
        target=log_consume_loop,
        args=(
            event_consumer,
            settings.logs_topic,
            settings.kafka_min_commit_count,
            logs_value_deserializer,
            logs_value_context,
            output_producer,
            settings.output_topic,
            registration_state_table,
            broadcaster_events_table,
            registration_state_lock,
        ),
    )

    logs_thread.start()

if settings.processing_mode == Mode.TRANSACTION:
    tx_thread = Thread(
        target=transaction_consume_loop,
        args=(
            event_consumer,
            settings.transactions_topic,
            settings.kafka_min_commit_count,
            transactions_value_deserializer,
            transactions_value_context,
            output_producer,
            settings.output_topic,
            registration_state_table,
            broadcaster_events_table,
            registration_state_lock,
        ),
    )

    tx_thread.start()
