from enum import Enum


class KafkaTopic(Enum):
    CREATED = 1
    EXISTS = 2
    ERROR = 3
    UNKNOWN = 4


def all_exist(topic_enums):
    if any(not isinstance(v, KafkaTopic) for v in topic_enums):
        raise ValueError('Only KafkaTopic enumerated values supported')
    return all(v == KafkaTopic.EXISTS or v == KafkaTopic.CREATED for v in topic_enums)


def parse_kafka_topic_args():
    from argparse import ArgumentParser
    from mccode_plumber import __version__
    parser = ArgumentParser(description="Prepare the named Kafka broker to host one or more topics")
    parser.add_argument('-b', '--broker', type=str, help='The Kafka broker server to interact with')
    parser.add_argument('topic', nargs="+", type=str, help='The Kafka topic(s) to register')
    parser.add_argument('-q', '--quiet', action='store_true', help='Quiet (positive) failure')
    parser.add_argument('-v', '--version', action='version', version=__version__)

    args = parser.parse_args()
    return args


def register_kafka_topics(broker: str, topics: list[str]):
    from confluent_kafka.admin import AdminClient, NewTopic
    client = AdminClient({"bootstrap.servers": broker})
    config = {
        # 'cleanup.policy': 'delete',
        # 'delete.retention.ms': 60000,
        'max.message.bytes': 104857600,
        # 'retention.bytes': 10737418240,
        # 'retention.ms': 30000,
        # 'segment.bytes': 104857600,
        # 'segment.ms': 60000
    }
    new_ts = [NewTopic(t, num_partitions=1, replication_factor=1, config=config) for t in topics]
    futures = client.create_topics(new_ts)
    results = {}
    for topic, future in futures.items():
        try:
            future.result()
            results[topic] = KafkaTopic.CREATED
        except Exception as e:
            from confluent_kafka.error import KafkaError
            if e.args[0] == KafkaError.TOPIC_ALREADY_EXISTS:
                results[topic] = KafkaTopic.EXISTS
            else:
                results[topic] = e.args[0]
    return results


def register_topics():
    args = parse_kafka_topic_args()
    results = register_kafka_topics(args.broker, args.topic)
    if not args.quiet:
        for topic, result in results.items():
            if result == KafkaTopic.CREATED:
                print(f'Created topic {topic}')
            elif result == KafkaTopic.EXISTS:
                print(f'Topic {topic} already exists')
            else:
                print(f'Failed to register topic "{topic}"? {result}')
