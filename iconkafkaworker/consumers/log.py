import sys
from json import dumps

from confluent_kafka import KafkaError, KafkaException


def log_consume_loop(
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
    The log consumption loop.

    :param broadcaster_event_state:
    :param consumer: A Kafka consumer object.
    :param consume_topic: The logs topic.
    :type consume_topic: str
    :param min_commit: The minimum number of messages to process before forcing the consumer to send a commit message.
    :type min_commit: int
    :param value_deserializer: A Kafka deserializer object that is appropriate for the registration message value.
    :param value_context: A Kafka SerializationContext object that is appropriate for the topic and field.
    :param producer: A Kafka producer object.
    :param produce_topic: The output topic for processed logs.
    :type produce_topic: str
    :param registration_state: The registration state.
    :type registration_state: dict
    :param lock: The shared lock object between the two consumption threads.
    :return: None
    """

    # Subscribe the consumer to the topic
    # Do not need a callback since this will be part of a consumer group and we should let the broker handle assignments
    consumer.subscribe([consume_topic])

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
            log_msg_handler(
                msg,
                value_deserializer,
                value_context,
                producer,
                produce_topic,
                registration_state,
                broadcaster_event_state,
                lock,
            )

            msg_count += 1

            # If we have processed enough messages, perform a synchronous commit to the broker
            if msg_count % min_commit == 0:
                consumer.commit(asynchronous=False)


def log_msg_handler(
    msg,
    deserializer,
    context,
    producer,
    topic,
    registration_state,
    broadcaster_event_state,
    lock,
):
    """
    Handler for log messages.


    :param broadcaster_event_state:
    :param msg: A Kakfa message object with the log message content.
    :param deserializer: A Kafka deserializer object that is appropriate for the registration message value.
    :param context: A Kafka SerializationContext object that is appropriate for the topic and field.
    :param producer: A Kafka producer object.
    :param topic: The name of the output topic.
    :type topic: str
    :param registration_state: The registration state.
    :type registration_state: dict
    :param lock: The shared lock object between the two consumption threads.
    :return: None
    """

    # We will always have a value, so we can just unpack it
    value = deserializer(msg.value(), context)

    # Acquire state lock because we're going to start looking things up
    lock.acquire()

    # Check first to see if address is in the state, and if not, release lock and exit
    if value["address"] not in registration_state:
        lock.release()
        return

    # Next, check if there is anything in the message's indexed field, and if not, release lock and exit
    if not value["indexed"]:
        lock.release()
        return

    # We can now unpack the content of the indexed field
    indexed = value["indexed"]

    # The first field will always be the keyword and type signature--"Keyword(type,type,type)"
    # so we can split on "(" to pull out just the keyword
    keyword = indexed[0].split("(")[0]

    # Last check to the state to make sure the keyword is present
    if keyword not in registration_state[value["address"]]:
        lock.release()
        return

    # Last state operation: pull out the position of interest and keep it for later
    for position, event_ids in registration_state[value["address"]][keyword].items():

        # Prepare and produce the message
        response_value = indexed[position]
        broadcasters = [broadcaster_event_state[event] for event in event_ids]

        response_message = {
            "address": value["address"],
            "keyword": keyword,
            "position": position,
            "value": response_value,
        }

        producer.produce(
            topic=topic,
            key=bytes(str(broadcasters), "utf-8"),
            value=dumps(response_message),
        )

    lock.release()
