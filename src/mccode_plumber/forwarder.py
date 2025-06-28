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
    from streaming_data_types.forwarder_config_update_fc00 import StreamInfo, Protocol
    return [StreamInfo(pv['source'], pv['module'], pv['topic'], Protocol.Protocol.PVA, periodic=False) for pv in pvs]


def configure_forwarder(pvs: list[dict], config=None, prefix=None, topic=None):
    from confluent_kafka import Producer
    from streaming_data_types.forwarder_config_update_fc00 import serialise_fc00 as serialise
    from streaming_data_types.fbschemas.forwarder_config_update_fc00.UpdateType import UpdateType

    cfg_broker, cfg_topic, pvs = normalise_pvs(pvs, config, prefix, topic)
    producer = Producer({"bootstrap.servers": cfg_broker})
    producer.produce(cfg_topic, serialise(UpdateType.ADD, streams(pvs)))
    producer.flush()
    return pvs


def reset_forwarder(pvs: list[dict], config=None, prefix=None, topic=None):
    from confluent_kafka import Producer
    from streaming_data_types.forwarder_config_update_fc00 import serialise_fc00 as serialise
    from streaming_data_types.fbschemas.forwarder_config_update_fc00.UpdateType import UpdateType

    cfg_broker, cfg_topic, pvs = normalise_pvs(pvs, config, prefix, topic)
    producer = Producer({"bootstrap.servers": cfg_broker})
    producer.produce(cfg_topic, serialise(UpdateType.REMOVE, streams(pvs)))
    producer.flush()
    return pvs


def forwarder_partial_streams(prefix, topic, parameters):
    names = [p.name for p in parameters]
    if 'mcpl_filename' not in names:
        names.append("mcpl_filename")
    # Minimal information used by the forwarder for stream setup:
    partial = [dict(source=f'{prefix}{n}', module='f144', topic=topic) for n in names]
    return partial


def parse_registrar_args():
    from argparse import ArgumentParser
    from .mccode import get_mccode_instr_parameters
    from mccode_plumber import __version__

    parser = ArgumentParser(description="Discover EPICS PVs and inform a forwarder about them")
    parser.add_argument('-p', '--prefix', type=str, default='mcstas:')
    parser.add_argument('instrument', type=str, help="The mcstas instrument with EPICS PVs")
    parser.add_argument('-c', '--config', type=str, help="The Kafka server and topic for configuring the Forwarder")
    parser.add_argument('-t', '--topic', type=str, help="The Kafka topic to instruct the Forwarder to send data to")
    parser.add_argument('-v', '--version', action='version', version=__version__)

    args = parser.parse_args()
    params = forwarder_partial_streams(args.prefix, args.topic, get_mccode_instr_parameters(args.instrument))
    return params, args


def setup():
    parameters, args = parse_registrar_args()
    configure_forwarder(parameters, config=args.config, prefix=args.prefix, topic=args.topic)


def teardown():
    parameters, args = parse_registrar_args()
    reset_forwarder(parameters, config=args.config, prefix=args.prefix, topic=args.topic)
