import sys

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
                registration_state,
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
    registration_state,
    broadcaster_event_state,
    lock,
):
    """

    :param broadcaster_event_state:
    :param msg:
    :param deserializer:
    :param context:
    :param producer:
    :param topic:
    :param registration_state:
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

    if to_address not in registration_state:
        lock.release()
        return

    if from_address not in registration_state[to_address]:
        lock.release()
        return

    lock.release()

    broadcasters = [
        broadcaster_event_state[event]
        for event in registration_state[to_address][from_address]
    ]

    producer.produce(
        topic=topic,
        key=bytes(str(broadcasters), "utf-8"),
        value=value,
    )
