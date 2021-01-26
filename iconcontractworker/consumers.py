import sys
from json import dumps

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.cimpl import OFFSET_END


def registration_on_assign(consumer, partitions):
    for p in partitions:
        p.offset = OFFSET_END
    consumer.assign(partitions)


def registration_consume_loop(
    consumer, topic, min_commit, value_deserializer, value_context, state, lock
):
    consumer.subscribe([topic], on_assign=registration_on_assign)
    msg_count = 0
    while True:
        msg = consumer.poll(timeout=1)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write(
                    "%% %s [%d] reached end at offset %d\n"
                    % (msg.topic(), msg.partition(), msg.offset())
                )
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            registration_msg_handler(
                msg, value_deserializer, value_context, state, lock
            )
            msg_count += 1
            if msg_count % min_commit == 0:
                consumer.commit(asynchronous=False)


def registration_msg_handler(msg, deserializer, context, state, lock):
    if msg.value():
        value = deserializer(msg.value(), context)
        lock.acquire()
        if value["address"] not in state:
            state[value["address"]] = {value["keyword"]: value["position"]}
        else:
            state[value["address"]][value["keyword"]] = value["position"]
        lock.release()
    else:
        key = msg.key().decode("utf-8")
        address = key[0:42]
        keyword = key[42:]
        lock.acquire()
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
    consumer.subscribe([consume_topic])

    msg_count = 0
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write(
                    "%% %s [%d] reached end at offset %d\n"
                    % (msg.topic(), msg.partition(), msg.offset())
                )
            elif msg.error():
                raise KafkaException(msg.error())
        else:
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
            if msg_count % min_commit == 0:
                consumer.commit(asynchronous=False)


def log_msg_handler(msg, deserializer, context, producer, topic, state, lock):
    value = deserializer(msg.value(), context)

    lock.acquire()

    if value["address"] not in state:
        lock.release()
        return

    if not value["indexed"]:
        lock.release()
        return

    indexed = value["indexed"]
    keyword = indexed[0].split("(")[0]
    if keyword not in state[value["address"]]:
        lock.release()
        return

    position = state[value["address"]][keyword]

    lock.release()

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

    return
