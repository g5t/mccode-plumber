"""
Control a running Forwarder instance to send data to a Kafka broker.

Two gateway functions are exposed as system scripts to add and remove an Instr's parameters from the Forwarder's
list of EPICS PVs to monitor.

Alternatively, the same functionality can be accessed from Python using the configure_forwarder and reset_forwarder
functions. Which take PV information and Forwarder/Kafka configuration as arguments.
"""


def normalise_pvs(pvs: list[dict], config=None, prefix=None, topic=None):
    if config is None:
        config = "localhost:9092/forwarderConfig"
    if prefix is None:
        prefix = 'mcstas:'
    if topic is None:
        topic = 'mcstasParameters'

    if '/' not in config:
        raise RuntimeError('Expected / to separate broker and topic in Forwarder Kafka configuration specification')

    cfg_broker, cfg_topic = config.split('/', 1)

    for pv in pvs:
        if 'source' not in pv:
            pv['source'] = f'{prefix}{pv["name"]}'
        if 'topic' not in pv:
            pv['topic'] = topic
    return cfg_broker, cfg_topic, pvs


def streams(pvs: list[dict]):
    from streaming_data_types.forwarder_config_update_rf5k import StreamInfo, Protocol
    return [StreamInfo(pv['source'], pv['module'], pv['topic'], Protocol.Protocol.PVA) for pv in pvs]


def configure_forwarder(pvs: list[dict], config=None, prefix=None, topic=None):
    from confluent_kafka import Producer
    from streaming_data_types.forwarder_config_update_rf5k import serialise_rf5k, StreamInfo, Protocol
    from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import UpdateType

    cfg_broker, cfg_topic, pvs = normalise_pvs(pvs, config, prefix, topic)
    producer = Producer({"bootstrap.servers": cfg_broker})
    producer.produce(cfg_topic, serialise_rf5k(UpdateType.ADD, streams(pvs)))
    producer.flush()
    return pvs


def reset_forwarder(pvs: list[dict], config=None, prefix=None, topic=None):
    from confluent_kafka import Producer
    from streaming_data_types.forwarder_config_update_rf5k import serialise_rf5k
    from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import UpdateType

    cfg_broker, cfg_topic, pvs = normalise_pvs(pvs, config, prefix, topic)
    producer = Producer({"bootstrap.servers": cfg_broker})
    producer.produce(cfg_topic, serialise_rf5k(UpdateType.REMOVE, streams(pvs)))
    producer.flush()
    return pvs


def parse_registrar_args():
    from argparse import ArgumentParser
    from mccode.loader.loader import parse_mccode_instr_parameters

    parser = ArgumentParser(description="Discover EPICS PVs and inform a forwarder about them")
    parser.add_argument('-p', '--prefix', type=str, default='mcstas:')
    parser.add_argument('instrument', type=str, help="The mcstas instrument with EPICS PVs")
    parser.add_argument('-c', '--config', type=str, help="The Kafka server and topic for configuring the Forwarder")
    parser.add_argument('-t', '--topic', type=str, help="The Kafka topic to instruct the Forwarder to send data to")

    args = parser.parse_args()
    with open(args.instrument, 'r') as file:
        contents = file.read()
    parameters = parse_mccode_instr_parameters(contents)
    # the forwarder only cares about: "source", "module", "topic"
    params = [{'source': f'{args.prefix}{p.name}', 'module': 'f144', 'topic': args.topic} for p in parameters]
    return params, args


def setup():
    parameters, args = parse_registrar_args()
    configure_forwarder(parameters, config=args.config, prefix=args.prefix, topic=args.topic)


def teardown():
    parameters, args = parse_registrar_args()
    reset_forwarder(parameters, config=args.config, prefix=args.prefix, topic=args.topic)