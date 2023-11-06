def parse_kafka_topic_args():
    from argparse import ArgumentParser
    parser = ArgumentParser(description="Prepare the named Kafka broker to host one or more topics")
    parser.add_argument('-b', '--broker', type=str, help='The Kafka broker server to interact with')
    parser.add_argument('topic', nargs="+", type=str, help='The Kafka topic(s) to register')

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
            print(f"Failed to create topic {topic}: {e}")
