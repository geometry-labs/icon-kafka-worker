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
import os
import sys
from json import loads

from confluent_kafka import KafkaError, KafkaException

from iconkafkaworker.consumers.utils import registration_on_assign
from iconkafkaworker.settings import Mode, settings


def registration_consume_loop(
    processing_mode,
    consumer,
    topics,
    min_commit,
    value_deserializer,
    value_context,
    registration_state,
    broadcaster_event_state,
    reverse_search_dict,
    lock,
):
    """
    The registration consumption loop.

    :param reverse_search_dict:
    :param broadcaster_event_state:
    :param processing_mode:
    :param consumer: A Kafka consumer object
    :param topics: List of registration-related topics. Must include the registration events and broadcaster events topics.
    :type topics: List[str]
    :param min_commit: The minimum number of messages to process before forcing the consumer to send a commit message
    :type min_commit: int
    :param value_deserializer: A Kafka deserializer object that is appropriate for the registration message value
    :param value_context: A Kafka SerializationContext object that is appropriate for the topic and field
    :param registration_state: The registration state
    :type registration_state: dict
    :param lock: The shared lock object between the two consumption threads
    :return: None
    """

    logging.info("Registration consumer thread started")

    # Subscribe the consumer to the topic, and use the registration_on_assign callback to set the consumer to the end
    # of the topic
    consumer.subscribe(topics, on_assign=registration_on_assign)

    if settings.PROCESSING_MODE == Mode.TRANSACTION:
        to_from_pairs_state, from_to_pairs_state = registration_state

    msg_count = 0

    while True:
        # Poll for a message
        msg = consumer.poll(timeout=1)

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
            if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                sys.stderr.write("Kafka topic not ready. Restarting.")
                os._exit(1)
            # If there has been some other error, raise that
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Otherwise, we have a message and can pass it to the handler
            if processing_mode == Mode.CONTRACT:
                registration_msg_contract_mode_handler(
                    msg,
                    value_deserializer,
                    value_context,
                    registration_state,
                    broadcaster_event_state,
                    reverse_search_dict,
                    lock,
                )
            elif processing_mode == Mode.TRANSACTION:
                registration_msg_transaction_mode_handler(
                    msg,
                    value_deserializer,
                    value_context,
                    to_from_pairs_state,
                    from_to_pairs_state,
                    broadcaster_event_state,
                    reverse_search_dict,
                    lock,
                )
            else:
                raise ValueError(
                    "The provided processing mode, {}, is not valid.".format(
                        processing_mode
                    )
                )

            msg_count += 1

            # If we have processed enough messages, perform a synchronous commit to the broker
            if msg_count % min_commit == 0:
                consumer.commit(asynchronous=False)


def registration_msg_transaction_mode_handler(
    msg,
    deserializer,
    context,
    to_from_pairs_state,
    from_to_pairs_state,
    broadcaster_event_state,
    reverse_search_dict,
    lock,
):
    """

    :param from_to_pairs_state:
    :param to_from_pairs_state:
    :param reverse_search_dict:
    :param broadcaster_event_state:
    :param msg:
    :param deserializer:
    :param context:
    :param lock:
    :return:
    """

    if msg.topic() == settings.REGISTRATIONS_TOPIC:
        # Check to see if it is a registration (has a value) or a deregistration (has no value)
        if msg.value():
            # We have a value, so we will unpack it
            value = deserializer(msg.value(), context)
            reg_id = msg.key().decode("utf-8")

            # Acquire lock to make sure no log processing happens while we update
            lock.acquire()

            if not value["from_address"]:
                from_address = "*"
            else:
                from_address = value["from_address"]

            if not value["to_address"]:
                to_address = "*"
            else:
                to_address = value["to_address"]

            # Insert into states

            reverse_search_dict[reg_id] = (to_address, from_address)

            if to_address not in to_from_pairs_state:
                to_from_pairs_state[to_address] = {from_address: [reg_id]}
            else:
                if from_address not in to_from_pairs_state[to_address]:
                    to_from_pairs_state[to_address][from_address] = [reg_id]
                else:
                    to_from_pairs_state[to_address][from_address].append(reg_id)

            if from_address not in from_to_pairs_state:
                from_to_pairs_state[from_address] = {to_address: [reg_id]}
            else:
                if to_address not in from_to_pairs_state[from_address]:
                    from_to_pairs_state[from_address][to_address] = [reg_id]
                else:
                    from_to_pairs_state[from_address][to_address].append(reg_id)

            # Release the lock to unblock and then we're done
            lock.release()

        else:
            # We need to perform a deregistration, so unpack the message key
            reg_id = msg.key().decode("utf-8")
            to_address, from_address = reverse_search_dict[reg_id]

            if not from_address:
                from_address = "*"
            else:
                from_address = from_address

            if not to_address:
                to_address = "*"
            else:
                to_address = to_address

            # Acquire the state lock
            lock.acquire()

            # remove from state
            to_from_pairs_state[to_address][from_address].remove(reg_id)
            from_to_pairs_state[from_address][to_address].remove(reg_id)

            # remove from reverse search
            del reverse_search_dict[reg_id]

            lock.release()

    if msg.topic() == settings.BROADCASTER_EVENTS_TOPIC:
        value = loads(msg.value().decode("utf-8"))
        if value["active"]:
            lock.acquire()
            broadcaster_event_state[value["event_id"]] = value["broadcaster_id"]

        else:
            lock.acquire()
            del broadcaster_event_state[value["event_id"]]

        lock.release()


def registration_msg_contract_mode_handler(
    msg,
    deserializer,
    context,
    registration_state,
    broadcaster_event_state,
    reverse_search_dict,
    lock,
):
    """
    Handler for registration messages.

    :param reverse_search_dict:
    :param broadcaster_event_state:
    :param msg: A Kakfa message object with the registration message content.
    :param deserializer: A Kafka deserializer object that is appropriate for the registration message value.
    :param context: A Kafka SerializationContext object that is appropriate for the topic and field.
    :param registration_state: The registration state.
    :type registration_state: dict
    :param lock: The shared lock object between the two consumption threads.
    :return: None
    """

    if msg.topic() == settings.REGISTRATIONS_TOPIC:

        # Check to see if it is a registration (has a value) or a deregistration (has no value)
        if msg.value():
            # We have a value, so we will unpack it
            value = deserializer(msg.value(), context)

            # Acquire lock to make sure no log processing happens while we update
            lock.acquire()

            to_address = value["to_address"]
            keyword = value["keyword"]
            position = value["position"]
            reg_id = msg.key().decode("utf-8")

            reverse_search_dict[reg_id] = (to_address, keyword, position)

            if to_address not in registration_state:
                registration_state[to_address] = {keyword: {position: [reg_id]}}
            else:
                if keyword not in registration_state[to_address]:
                    registration_state[to_address][keyword] = {position: [reg_id]}
                else:
                    if position not in registration_state[to_address][keyword]:
                        registration_state[to_address][keyword][position] = [reg_id]
                    else:
                        registration_state[to_address][keyword][position].append(reg_id)

            lock.release()
        else:
            # We need to perform a deregistration, so unpack the message key
            reg_id = msg.key().decode("utf-8")
            to_address, keyword, position = reverse_search_dict[reg_id]

            # Acquire the state lock
            lock.acquire()

            # remove from state
            registration_state[to_address][keyword][position].remove(reg_id)

            # remove from reverse search
            del reverse_search_dict[reg_id]

            lock.release()

    if msg.topic() == settings.BROADCASTER_EVENTS_TOPIC:
        value = loads(msg.value().decode("utf-8"))
        if value["active"]:
            lock.acquire()
            broadcaster_event_state[value["event_id"]] = value["broadcaster_id"]

        else:
            lock.acquire()
            del broadcaster_event_state[value["event_id"]]

        lock.release()
