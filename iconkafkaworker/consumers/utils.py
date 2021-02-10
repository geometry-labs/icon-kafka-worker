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
