import sys
from json import dumps

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.cimpl import OFFSET_END


def registration_on_assign(consumer, partitions):
    """
    Callback function to set consumer position to the end of the topic.

    :param consumer: A Kafka consumer object
    :param partitions: A list of TopicPartitions
    :return: None
    """
    for p in partitions:
        p.offset = OFFSET_END
    consumer.assign(partitions)


def registration_consume_loop(
    consumer, topic, min_commit, value_deserializer, value_context, state, lock
):
    """
    The registration consumption loop.

    :param consumer: A Kafka consumer object
    :param topic: The registration topic
    :type topic: str
    :param min_commit: The minimum number of messages to process before forcing the consumer to send a commit message
    :type min_commit: int
    :param value_deserializer: A Kafka deserializer object that is appropriate for the registration message value
    :param value_context: A Kafka SerializationContext object that is appropriate for the topic and field
    :param state: The registration state
    :type state: dict
    :param lock: The shared lock object between the two consumption threads
    :return: None
    """

    # Subscribe the consumer to the topic, and use the registration_on_assign callback to set the consumer to the end
    # of the topic
    consumer.subscribe([topic], on_assign=registration_on_assign)

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
            # If there has been some other error, raise that
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Otherwise, we have a message and can pass it to the handler
            registration_msg_handler(
                msg, value_deserializer, value_context, state, lock
            )

            msg_count += 1

            # If we have processed enough messages, perform a synchronous commit to the broker
            if msg_count % min_commit == 0:
                consumer.commit(asynchronous=False)


def registration_msg_handler(msg, deserializer, context, state, lock):
    """
    Handler for registration messages.

    :param msg: A Kakfa message object with the registration message content.
    :param deserializer: A Kafka deserializer object that is appropriate for the registration message value.
    :param context: A Kafka SerializationContext object that is appropriate for the topic and field.
    :param state: The registration state.
    :type state: dict
    :param lock: The shared lock object between the two consumption threads.
    :return: None
    """

    # Check to see if it is a registration (has a value) or a deregistration (has no value)
    if msg.value():
        # We have a value, so we will unpack it
        value = deserializer(msg.value(), context)

        # Acquire lock to make sure no log processing happens while we update
        lock.acquire()

        if value["address"] not in state:
            # If the address isn't in the top level dictionary, we can directly insert the whole object
            state[value["address"]] = {value["keyword"]: value["position"]}
        else:
            # Otherwise we just insert a new keyword:position value
            state[value["address"]][value["keyword"]] = value["position"]

        # Release the lock to unblock and then we're done
        lock.release()
    else:
        # We need to perform a deregistration, so unpack the message key
        key = msg.key().decode("utf-8")

        # Because the value is None, we have to use the key to determine what was removed
        address = key[0:42]
        keyword = key[42:]

        # Acquire the state lock
        lock.acquire()

        # Directly delete the keyword from the dictionary
        # If it was the only thing in that address, we will still return False if we do bool(state[address])
        # so no further deletions are required
        if state[address][keyword]:
            del state[address][keyword]

        lock.release()


def log_consume_loop(
    consumer,
    consume_topic,
    min_commit,
    value_deserializer,
    value_context,
    producer,
    produce_topic,
    state,
    lock,
):
    """
    The log consumption loop.

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
    :param state: The registration state.
    :type state: dict
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
                state,
                lock,
            )

            msg_count += 1

            # If we have processed enough messages, perform a synchronous commit to the broker
            if msg_count % min_commit == 0:
                consumer.commit(asynchronous=False)


def log_msg_handler(msg, deserializer, context, producer, topic, state, lock):
    """
    Handler for log messages.


    :param msg: A Kakfa message object with the log message content.
    :param deserializer: A Kafka deserializer object that is appropriate for the registration message value.
    :param context: A Kafka SerializationContext object that is appropriate for the topic and field.
    :param producer: A Kafka producer object.
    :param topic: The name of the output topic.
    :type topic: str
    :param state: The registration state.
    :type state: dict
    :param lock: The shared lock object between the two consumption threads.
    :return: None
    """

    # We will always have a value, so we can just unpack it
    value = deserializer(msg.value(), context)

    # Acquire state lock because we're going to start looking things up
    lock.acquire()

    # Check first to see if address is in the state, and if not, release lock and exit
    if value["address"] not in state:
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
    if keyword not in state[value["address"]]:
        lock.release()
        return

    # Last state operation: pull out the position of interest and keep it for later
    position = state[value["address"]][keyword]

    # Release the lock since we'll just be producing a message now
    lock.release()

    # Prepare and produce the message
    response_value = indexed[position]
    response_key = value["address"]

    response_message = {
        "address": value["address"],
        "keyword": keyword,
        "position": position,
        "value": response_value,
    }

    producer.produce(
        topic=topic, key=bytes(response_key, "utf-8"), value=dumps(response_message)
    )
