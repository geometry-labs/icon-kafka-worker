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

import sys
from json import dumps

from confluent_kafka import KafkaError, KafkaException


def transaction_consume_loop(
    consumer,
    consume_topic,
    min_commit,
    value_deserializer,
    value_context,
    producer,
    produce_topic,
    registration_state,
    broadcaster_event_state,
    lock,
):
    """

    :param broadcaster_event_state:
    :param consumer:
    :param consume_topic:
    :param min_commit:
    :param value_deserializer:
    :param value_context:
    :param producer:
    :param produce_topic:
    :param registration_state:
    :param lock:
    :return:
    """
    # Subscribe the consumer to the topic
    # Do not need a callback since this will be part of a consumer group and we should let the broker handle assignments
    consumer.subscribe([consume_topic])

    to_from_pairs_state, from_to_pairs_state = registration_state

    msg_count = 0
    while True:
        # Poll for a message
        msg = consumer.poll(timeout=1.0)

        # If no new message, try again
        if msg is None:
            continue

        if msg.error():
            # If we have reached the end of the partition, log that
            if msg.error().code() == KafkaError._PARTITION_EOF:
                sys.stderr.write(
                    "%% %s [%d] reached end at offset %d\n"
                    % (msg.topic(), msg.partition(), msg.offset())
                )
            # If there has been some other error, raise that
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Otherwise, we have a message and can pass it to the handler
            transaction_msg_handler(
                msg,
                value_deserializer,
                value_context,
                producer,
                produce_topic,
                to_from_pairs_state,
                from_to_pairs_state,
                broadcaster_event_state,
                lock,
            )

            msg_count += 1

            # If we have processed enough messages, perform a synchronous commit to the broker
            if msg_count % min_commit == 0:
                consumer.commit(asynchronous=False)


def transaction_msg_handler(
    msg,
    deserializer,
    context,
    producer,
    topic,
    to_from_pairs_state,
    from_to_pairs_state,
    broadcaster_event_state,
    lock,
):
    """

    :param from_to_pairs_state:
    :param to_from_pairs_state:
    :param broadcaster_event_state:
    :param msg:
    :param deserializer:
    :param context:
    :param producer:
    :param topic:
    :param lock:
    :return:
    """
    # We will always have a value, so we can just unpack it
    value = deserializer(msg.value(), context)

    if not value["from_address"]:
        from_address = "*"
    else:
        from_address = value["from_address"]

    if not value["to_address"]:
        to_address = "*"
    else:
        to_address = value["to_address"]

    # Acquire state lock because we're going to start looking things up
    lock.acquire()

    try:
        exact_to_from_broadcasters = {
            broadcaster_event_state[event]
            for event in to_from_pairs_state[to_address][from_address]
        }
    except KeyError:
        exact_to_from_broadcasters = set()

    try:
        wildcard_to_broadcasters = {
            broadcaster_event_state[event]
            for event in to_from_pairs_state["*"][from_address]
        }

    except KeyError:
        wildcard_to_broadcasters = set()

    try:
        wildcard_from_broadcasters = {
            broadcaster_event_state[event]
            for event in from_to_pairs_state["*"][to_address]
        }

    except KeyError:
        wildcard_from_broadcasters = set()

    lock.release()

    broadcasters = exact_to_from_broadcasters.union(wildcard_to_broadcasters).union(
        wildcard_from_broadcasters
    )

    if broadcasters:
        producer.produce(
            topic=topic,
            key=bytes(str(list(broadcasters)), "utf-8"),
            value=dumps(value),
        )
