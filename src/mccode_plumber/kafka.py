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


def register_topics():
    from confluent_kafka.admin import AdminClient, NewTopic
    args = parse_kafka_topic_args()

    client = AdminClient({"bootstrap.servers": args.broker})
    topics = [NewTopic(t, num_partitions=1, replication_factor=1) for t in args.topic]
    futures = client.create_topics(topics)

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic {topic} created")
        except Exception as e:
            from confluent_kafka.error import KafkaError
            if not (args.quiet and e.args[0] == KafkaError.TOPIC_ALREADY_EXISTS):
                print(f"Failed to create topic {topic}: {e.args[0].str()}")
