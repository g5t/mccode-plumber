from pathlib import Path
from mccode_antlr.instr import Instr


def guess_instr_config(name: str):
    from os import access, R_OK
    guess = f'/event-formation-unit/configs/{name}/configs/{name}.json'
    if access(guess, R_OK):
        return guess
    raise FileNotFoundError(f'Could not find {guess} in {name}')

def guess_instr_calibration(name: str):
    from os import access, R_OK
    guess = f'/event-formation-unit/configs/{name}/configs/{name}nullcalib.json'
    if access(guess, R_OK):
        return guess
    raise FileNotFoundError(f'Could not find {guess} in {name}')

def guess_instr_efu(name: str):
    from os import access, X_OK
    guess = name.split('_')[0].split('.')[0].split('-')[0].lower()
    if access(guess, X_OK):
        return guess
    raise FileNotFoundError(f'Could not find {guess} in {name}')


def register_topics(broker: str, topics: list[str]):
    """Ensure that topics are registered in the Kafka broker."""
    from mccode_plumber.kafka import register_kafka_topics, all_exist
    res = register_kafka_topics(broker, topics)
    if not all_exist(res.values()):
        raise RuntimeError(f'Missing Kafka topics? {res}')


def configure_forwarder(instr: Instr, broker: str, config: str, prefix: str, topic: str):
    """Configure forwarder by sending a configuration broker ADD streams for parameters

    Parameters
    ----------
    instr : Instr
        the source of parameters to build stream information for
    broker : str
        the Kafka broker that hosts the configuration and topic streams
    config : str
        the configuration stream name
    prefix : str
        the prefix to add to the stream source name for each parameter
    topic : str
        the Kafka topic to which updates will be sent, at the configuration broker
    """
    from mccode_plumber.forwarder import (
        configure_forwarder as cf, forwarder_partial_streams as fps
    )
    cf(fps(prefix, topic, instr.parameters), f'{broker}/{config}', prefix, topic)


def deconfigure_forwarder(instr: Instr, broker: str, config: str, prefix: str, topic: str):
    """Stop forwarder by sending a configuration broker REMOVE streams for parameters

        Parameters
        ----------
        instr : Instr
            the source of parameters to build stream information for
        broker : str
            the Kafka broker that hosts the configuration and topic streams
        config : str
            the configuration stream name
        prefix : str
            the prefix to add to the stream source name for each parameter
        topic : str
            the Kafka topic to which updates will no longer be sent, at the configuration broker
        """
    from mccode_plumber.forwarder import (
        reset_forwarder as rf, forwarder_partial_streams as fps
    )
    rf(fps(prefix, topic, instr.parameters), f'{broker}/{config}', prefix, topic)


def augment_structure(instr: Instr, structure: dict, title: str, prefix: str, topic: str):
    """Helper to add stream JSON entries for Instr parameters to a NexusStructure

    Parameters
    ----------
    instr : Instr
        Instrument which contains one or more runtime parameter
    structure : dict
        NexusStructure JSON representing the instrument
    title : str
        Informative string about the simulation, to be inserted in structure
    prefix : str
        EPICS prefix used to construct PV addresses from parameter names, this
        is used here to construct the Kafka stream source as well
    topic : str
        the Kafka stream topic which will hold Forwarded PV updates
    """
    from mccode_plumber.writer import (
        add_title_to_nexus_structure,  add_pvs_to_nexus_structure,
        construct_writer_pv_dicts_from_parameters,
    )
    pvs = construct_writer_pv_dicts_from_parameters(instr.parameters, prefix, topic)
    data = add_pvs_to_nexus_structure(structure, pvs)
    data = add_title_to_nexus_structure(data, title)
    return data


def stop_writer(broker, job_topic, command_topic, job_id, timeout):
    from time import sleep
    from mccode_plumber.file_writer_control import WorkerJobPool
    pool = WorkerJobPool(f'{broker}/{job_topic}', f'{broker}/{command_topic}')
    sleep(timeout)
    pool.try_send_stop_now(None, job_id)


def start_writer(start_time, structure, filename, broker, job_topic, command_topic,
                 timeout):
    from uuid import uuid4
    from mccode_plumber.writer import writer_start
    job_id = str(uuid4())
    success = False
    try:
        start, handler = writer_start(
            start_time.isoformat(), structure, filename=filename, stop_time_string=None,
            broker=broker, job_topic=job_topic, command_topic=command_topic,
            timeout=timeout, job_id=job_id, wait=False
        )
        success = start.is_done()
    except RuntimeError as e:
        if job_id in str(e):
            # starting the job failed, so try to kill it
            stop_writer(broker, job_topic, command_topic, job_id, timeout)

    return job_id, success


def get_topics(data: dict):
    topics = set()
    for k, v in data.items():
        if isinstance(v, dict):
            topics.update(get_topics(v))
        elif k == 'topic':
            topics.add(v)
    return topics


def load_structure_file(structure_file):
    from os import access, R_OK
    from json import load
    if not (structure_file.exists() and structure_file.is_file() and access(structure_file, R_OK)):
        raise ValueError('You must provide a nexus-structure file')
    with open(structure_file, 'r') as f:
        return load(f)


def make_parser():
    from argparse import BooleanOptionalAction
    from mccode_plumber import __version__
    from restage.splitrun import make_splitrun_parser
    parser = make_splitrun_parser()
    parser.prog = 'mp-splitrun-nexus'
    parser.add_argument('-v' ,'--version', action='version', version=__version__)
    # No need to specify the broker, or monitor source or topic names
    parser.add_argument('--work', type=str, default=None, help='Working directory')
    parser.add_argument('--structure', type=str, default=None, help='NeXus Structure JSON path')
    parser.add_argument('--structure-out', type=str, default=None, help='Output configured structure JSON path')
    parser.add_argument('--efu', type=str, default=None, help='EFU binary path/name')
    parser.add_argument('--config', type=str, default=None, help='EFU configuration JSON')
    parser.add_argument('--calibration', type=str, default=None, help='EFU calibration JSON')
    parser.add_argument('--manage', action=BooleanOptionalAction, default=True, help='Manage services locally')
    return parser


def main():
    from mccode_plumber.mccode import get_mcstas_instr
    from restage.splitrun import parse_splitrun
    from mccode_plumber.splitrun import monitors_to_kafka_callback_with_arguments
    args, parameters, precision = parse_splitrun(make_parser())
    instr = get_mcstas_instr(args.instrument[0])

    structure = load_structure_file(args.structure if args.structure else Path(args.instrument[0]).with_suffix('.json'))
    broker = 'localhost:9092'
    monitor_source = 'mccode-to-kafka'
    callback_topics = list(get_topics(structure))  # all structure-topics might be monitor topics?

    callback, callback_args = monitors_to_kafka_callback_with_arguments(broker, monitor_source, callback_topics)
    splitrun_kwargs = {
        'args': args, 'parameters': parameters, 'precision': precision,
        'callback': callback, 'callback_arguments': callback_args,
    }
    conduct_kwargs = {
        'efu': args.efu, 'config': args.config, 'calibration': args.calibration,
        'work': args.work, 'structure_out': args.structure_out, 'manage': args.manage,
    }
    for k in list(conduct_kwargs.keys()) + ['structure']:
        delattr(args, k)
    return conduct(instr, structure, broker, callback_topics, splitrun_kwargs, **conduct_kwargs)


def conduct(
        instr: Instr,
        structure,
        broker: str,
        callback_topics: list[str],
        splitrun_kwargs: dict,
        efu: str = None,
        config: str = None,
        calibration: str = None,
        work: str = None,
        structure_out: str = None,
        manage: bool = True,
):
    from datetime import datetime, timezone
    from restage.splitrun import splitrun_args
    from mccode_plumber.manage import (EventFormationUnit, EPICSMailbox, Forwarder, KafkaToNexus)

    # now = datetime.now(timezone.utc).isoformat(timespec='seconds').split('+')[0]
    now = datetime.now(timezone.utc)
    prefix = 'mcstas:'
    title = f'{instr.name} simulation {now}: {splitrun_kwargs["args"]}'
    filename = f'{instr.name}_{now:%y%m%dT%H%M%S}.h5'
    topics = {
        'parameter': 'SimulatedParameters',
        'event': 'SimulatedEvents',
        'config': 'ForwardConfig',
        'status': 'ForwardStatus',
        'command': 'WriterCommand',
        'job': 'WriterJob',
    }

    # Start up services if they should be managed locally
    services = () if not manage else (
        EventFormationUnit.start(
            binary=efu or guess_instr_efu(instr.name),
            config=config or guess_instr_config(name=instr.name),
            calibration=calibration or guess_instr_calibration(name=instr.name),
            broker=broker,
            topic=topics['event'],
        ),
        Forwarder.start(
            broker=broker,
            config=topics['config'],
            status=topics['status'],
        ),
        EPICSMailbox.start(
            parameters=instr.parameters,
            prefix=prefix,
        ),
        KafkaToNexus.start(
            broker=broker,
            work=work,
            command=topics['command'],
            job=topics['job'],
        ),
    )

    # Ensure stream topics exist
    register_topics(broker, list(topics.values()) + callback_topics)
    # Tell the forwarder what to forward
    configure_forwarder(instr, broker, topics['config'], prefix, topics['parameter'])

    # Create a file-writer job
    structure = augment_structure(instr, structure, title, prefix, topics['parameter'])
    if structure_out:
        from json import dump
        with open(structure_out, 'w') as f:
            dump(structure, f)

    job_id, success = start_writer(
        now, structure, filename, broker, topics['job'], topics['command'], 2.0,
    )

    # Do the actual simulation, calling into restage.splitrun after parsing,
    # Using the provided callbacks to send monitor data to Kafka
    splitrun_args(instr, **splitrun_kwargs)

    # Wait for the file-writer to finish its job (possibly kill it)
    stop_writer(broker, topics['job'], topics['command'], job_id, 2.0)

    # De-register the forwarder topics (Don't bother since we're about to kill it?)
    deconfigure_forwarder(instr, broker, topics['config'], prefix, topics['parameter'])

    for service in services:
        service.stop()

    ### Repack the resulting file into contiguous memory
    ## subprocess call to `h5repack -l CONTI ${filename} ${filename}.pack && mv ${filename}.pack ${filename}`

    ### Insert the HDF5 representation of the instrument into the file, just incase