from __future__ import annotations

from pathlib import Path

from mccode_antlr.common import InstrumentParameter
from mccode_antlr.instr import Instr
from mccode_plumber.manage.manager import ensure_readable_file, ensure_executable

def guess_instr_config(name: str):
    guess = f'/event-formation-unit/configs/{name}/configs/{name}.json'
    return ensure_readable_file(Path(guess))

def guess_instr_calibration(name: str):
    guess = f'/event-formation-unit/configs/{name}/configs/{name}nullcalib.json'
    return ensure_readable_file(Path(guess))

def guess_instr_efu(name: str):
    guess = name.split('_')[0].split('.')[0].split('-')[0].lower()
    return ensure_executable(Path(guess))


def register_topics(broker: str, topics: list[str]):
    """Ensure that topics are registered in the Kafka broker."""
    from mccode_plumber.kafka import register_kafka_topics, all_exist
    res = register_kafka_topics(broker, topics)
    if not all_exist(res.values()):
        raise RuntimeError(f'Missing Kafka topics? {res}')


def configure_forwarder(
        parameters: tuple[InstrumentParameter, ...],
        broker: str,
        config: str,
        prefix: str,
        topic: str
):
    """Configure forwarder by sending a configuration broker ADD streams for parameters

    Parameters
    ----------
    parameters: tuple[InstrumentParameter, ...]
        the parameters to build stream information for
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
    cf(fps(prefix, topic, parameters), f'{broker}/{config}', prefix, topic)


def deconfigure_forwarder(
        parameters: tuple[InstrumentParameter, ...],
        broker: str,
        config: str,
        prefix: str,
        topic: str
):
    """Stop forwarder by sending a configuration broker REMOVE streams for parameters

    Parameters
    ----------
    parameters: tuple[InstrumentParameter, ...]
        the parameters to build stream information for
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
    rf(fps(prefix, topic, parameters), f'{broker}/{config}', prefix, topic)


def augment_structure(
        parameters: tuple[InstrumentParameter,...],
        structure: dict,
        title: str,
        prefix: str,
        topic: str
):
    """Helper to add stream JSON entries for Instr parameters to a NexusStructure

    Parameters
    ----------
    parameters : tuple[InstrumentParameter,...]
        Instrument runtime parameters
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
    pvs = construct_writer_pv_dicts_from_parameters(parameters, prefix, topic)
    data = add_pvs_to_nexus_structure(structure, pvs)
    data = add_title_to_nexus_structure(data, title)
    return data


def stop_writer(broker, job_topic, command_topic, job_id, timeout):
    from time import sleep
    from mccode_plumber.file_writer_control import WorkerJobPool
    from mccode_plumber.file_writer_control.JobStatus import JobState
    pool = WorkerJobPool(f'{broker}/{job_topic}', f'{broker}/{command_topic}')
    sleep(timeout)
    pool.try_send_stop_now(None, job_id)
    state = pool.get_job_state(job_id)
    while state != JobState.DONE and state != JobState.ERROR and state != JobState.TIMEOUT:
        sleep(0.1)
        state = pool.get_job_state(job_id)



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


def load_file_json(file: str | Path):
    from mccode_plumber.manage.manager import ensure_readable_file
    from json import load
    file = ensure_readable_file(file)
    with file.open('r') as f:
        return load(f)


def get_instr_name_and_parameters(file: str | Path):
    from mccode_plumber.manage.manager import ensure_readable_file
    file = ensure_readable_file(file)

    if file.suffix == '.h5':
        # Shortcut loading the whole Instr:
        import h5py
        from mccode_antlr.io.hdf5 import HDF5IO
        with h5py.File(file, 'r', driver='core', backing_store=False) as f:
            name = f.attrs['name']
            parameters = HDF5IO.load(f['parameters'])
        return name, parameters
    elif file.suffix == '.instr':
        # No shortcuts
        from mccode_antlr.loader import load_mcstas_instr
        instr = load_mcstas_instr(file)
        return instr.name, instr.parameters

    raise ValueError('Unsupported file extension')


def make_services_parser():
    from mccode_plumber import __version__
    from argparse import ArgumentParser
    parser = ArgumentParser('mp-nexus-services')
    parser.add_argument('instrument', type=str, help='Instrument .instr or .h5 file')
    parser.add_argument('-v', '--version', action='version', version=__version__)
    # No need to specify the broker, or monitor source or topic names
    parser.add_argument('--structure', type=str, default=None, help='NeXus Structure JSON path')
    parser.add_argument('--efu', type=str, default=None, help='EFU binary path/name')
    parser.add_argument('--config', type=str, default=None, help='EFU configuration JSON')
    parser.add_argument('--calibration', type=str, default=None, help='EFU calibration JSON')
    return parser


def services():
    args = make_services_parser().parse_args()
    instr_name, instr_parameters = get_instr_name_and_parameters(args.instrument)
    kwargs = {
        'instr_name': instr_name,
        'instr_parameters': instr_parameters,
        'broker': 'localhost:9092',
        'efu': args.efu,
        'config': args.config,
        'calibration': args.calibration,
        'work': args.work,
    }
    load_in_wait_load_out(**kwargs)


def load_in_wait_load_out(
            instr_name: str,
            instr_parameters: tuple[InstrumentParameter, ...],
            broker: str,
            efu: str = None,
            config: str = None,
            calibration: str = None,
            work: str = None,
            manage: bool = True,
    ):
        import signal
        from mccode_plumber.manage import (
            EventFormationUnit, EPICSMailbox, Forwarder, KafkaToNexus
        )

        prefix = 'mcstas:'
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
                binary=efu or guess_instr_efu(instr_name),
                config=config or guess_instr_config(name=instr_name),
                calibration=calibration or guess_instr_calibration(name=instr_name),
                broker=broker,
                topic=topics['event'],
            ),
            Forwarder.start(
                broker=broker,
                config=topics['config'],
                status=topics['status'],
            ),
            EPICSMailbox.start(
                parameters=instr_parameters,
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
        register_topics(broker, list(topics.values()))

        def signal_handler(signum, frame):
            if signum == signal.SIGINT:
                print('Done waiting, following SIGINT')
                for service in services:
                    service.stop()
                exit(0)
            else:
                print(f'Received signal {signum}, ignoring')

        signal.signal(signal.SIGINT, signal_handler)
        print('Run `mp-nexus-splitrun` in another process now')
        print('Press Ctrl+C to exit')
        signal.pause()

        # In another process, now call orchestrate ...


def make_splitrun_nexus_parser():
    from mccode_plumber import __version__
    from restage.splitrun import make_splitrun_parser
    parser = make_splitrun_parser()
    parser.prog = 'mp-nexus-splitrun'
    parser.add_argument('-v' ,'--version', action='version', version=__version__)
    # No need to specify the broker, or monitor source or topic names
    parser.add_argument('--work', type=str, default=None, help='Working directory')
    parser.add_argument('--structure', type=str, default=None, help='NeXus Structure JSON path')
    parser.add_argument('--structure-out', type=str, default=None, help='Output configured structure JSON path')
    return parser

def main():
    from mccode_plumber.mccode import get_mcstas_instr
    from restage.splitrun import parse_splitrun
    from mccode_plumber.splitrun import monitors_to_kafka_callback_with_arguments
    args, parameters, precision = parse_splitrun(make_splitrun_nexus_parser())
    instr = get_mcstas_instr(args.instrument[0])

    structure = load_file_json(args.structure if args.structure else Path(args.instrument[0]).with_suffix('.json'))
    broker = 'localhost:9092'
    monitor_source = 'mccode-to-kafka'
    callback_topics = list(get_topics(structure))  # all structure-topics might be monitor topics?
    register_topics(broker, callback_topics) # ensure the topics are known to Kafka

    callback, callback_args = monitors_to_kafka_callback_with_arguments(broker, monitor_source, callback_topics)
    splitrun_kwargs = {
        'args': args, 'parameters': parameters, 'precision': precision,
        'callback': callback, 'callback_arguments': callback_args,
    }
    conduct_kwargs = {
        'work': args.work, 'structure_out': args.structure_out
    }
    for k in list(conduct_kwargs.keys()) + ['structure']:
        delattr(args, k)
    return orchestrate(instr, structure, broker, splitrun_kwargs, **conduct_kwargs)



def orchestrate(
        instr: Instr,
        structure,
        broker: str,
        splitrun_kwargs: dict,
        work: str = None,
        structure_out: str = None,
):
    from datetime import datetime, timezone
    from restage.splitrun import splitrun_args

    if not isinstance(work, Path):
        work = Path(work) if work else Path()

    # now = datetime.now(timezone.utc).isoformat(timespec='seconds').split('+')[0]
    # TODO Verify that UTC is the right time to use for file-writer start/stop times
    now = datetime.now(timezone.utc)
    prefix = 'mcstas:'
    title = f'{instr.name} simulation {now}: {splitrun_kwargs["args"]}'
    filename = work.resolve() / f'{instr.name}_{now:%y%m%dT%H%M%S}.h5'
    topics = {
        'parameter': 'SimulatedParameters',
        'config': 'ForwardConfig',
        'command': 'WriterCommand',
        'job': 'WriterJob',
    }

    # Tell the forwarder what to forward
    configure_forwarder(instr.parameters, broker, topics['config'], prefix, topics['parameter'])

    # Create a file-writer job
    structure = augment_structure(instr.parameters, structure, title, prefix, topics['parameter'])
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

    # Verify that the file has been written?
    path = ensure_readable_file(Path(filename))
    print(f'Finished writing {path}')

    # De-register the forwarder topics (Don't bother since we're about to kill it?)
    deconfigure_forwarder(instr.parameters, broker, topics['config'], prefix, topics['parameter'])
