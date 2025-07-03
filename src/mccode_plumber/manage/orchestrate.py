from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from mccode_antlr.common import InstrumentParameter
from mccode_antlr.instr import Instr
from mccode_plumber.manage import ensure_readable_file, ensure_writable_file, ensure_executable
from mccode_plumber.manage.efu import EventFormationUnitConfig

TOPICS = {
    'parameter': 'SimulatedParameters',
    'event': 'SimulatedEvents',
    'config': 'ForwardConfig',
    'status': 'ForwardStatus',
    'command': 'WriterCommand',
    'pool': 'WriterPool',
}
PREFIX = 'mcstas:'

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


def augment_structure(
        parameters: tuple[InstrumentParameter,...],
        structure: dict,
        title: str,
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
    """
    from mccode_plumber.writer import (
        add_title_to_nexus_structure,  add_pvs_to_nexus_structure,
        construct_writer_pv_dicts_from_parameters,
    )
    pvs = construct_writer_pv_dicts_from_parameters(parameters, PREFIX, TOPICS['parameter'])
    data = add_pvs_to_nexus_structure(structure, pvs)
    data = add_title_to_nexus_structure(data, title)
    return data


def stop_writer(broker, job_id, timeout):
    from time import sleep
    from datetime import timedelta
    from mccode_plumber.file_writer_control import WorkerJobPool
    from mccode_plumber.file_writer_control.JobStatus import JobState
    # The process is now told to switch to a 'control' topic, that is job-specific
    # So we should send the stop-command there. This is the 'command_topic_url'?
    def back_stop(job_topic, command_topic):
        job_topic_url = f"{broker}/{job_topic}"
        command_topic_url = f"{broker}/{command_topic}"
        pool = WorkerJobPool(job_topic_url, command_topic_url)
        sleep(1)
        pool.try_send_stop_now(None, job_id)
        state = pool.get_job_state(job_id)
        give_up = datetime.now() + timedelta(seconds=timeout)
        while state != JobState.DONE and state != JobState.ERROR and state != JobState.TIMEOUT and datetime.now() < give_up:
            sleep(1)
            state = pool.get_job_state(job_id)
        return state

    jstate = back_stop(TOPICS['pool'], TOPICS['command'])
    if jstate != JobState.DONE:
        print(f'Done trying to stop {job_id} -> {jstate}')


def start_writer(start_time: datetime,
                 structure: dict,
                 filename: Path,
                 broker: str,
                 timeout: float):
    from uuid import uuid1
    from mccode_plumber.writer import writer_start
    job_id = str(uuid1())
    success = False
    name = filename.name
    try:
        print(f"Starting {job_id} from {start_time} for file {name} under kafka-to-nexus' working directory")
        start, handler = writer_start(
            start_time.isoformat(), structure, filename=name,
            stop_time_string=None,
            broker=broker, job_topic=TOPICS['pool'], command_topic=TOPICS['command'],
            control_topic=TOPICS['command'], # don't switch topics
            timeout=timeout, job_id=job_id, wait=False
        )
        # success = start.is_done() # this causes an infinite hang?
        success = True
    except RuntimeError as e:
        if job_id in str(e):
            # starting the job failed, so try to kill it
            print(f"Starting {job_id} failed! Error: {e}")
            stop_writer(broker, job_id, timeout)

    return job_id, success


def get_topics_iter(data: list | tuple):
    topics = set()
    for entry in data:
        if isinstance(entry, dict):
            topics.update(get_topics_dict(entry))
        elif isinstance(entry, (list, tuple)):
            topics.update(get_topics_iter(entry))
    return topics


def get_topics_dict(data: dict):
    topics = set()
    for k, v in data.items():
        if isinstance(v, dict):
            topics.update(get_topics_dict(v))
        elif isinstance(v, (list, tuple)):
            topics.update(get_topics_iter(list(v)))
        elif k == 'topic':
            topics.add(v)
    return topics


def get_topics_json(data: dict) -> list[str]:
    """Traverse a loaded JSON object and return the found list of topic names"""
    return list(get_topics_dict(data))


def load_file_json(file: str | Path):
    from json import load
    file = ensure_readable_file(file)
    with file.open('r') as f:
        return load(f)


def get_instr_name_and_parameters(file: str | Path):
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
    elif file.suffix.lower() == '.json':
        # No shortcuts, but much faster
        from mccode_antlr.io.json import load_json
        instr = load_json(file)
        return instr.name, instr.parameters

    raise ValueError('Unsupported file extension')


def efu_parameter(s: str):
    if ':' in s:
        # with any ':' we require fully specified
        #  name:{name},binary:{binary},config:{config_path},calibration:{calibration_path},topic:{topic},port:{port}
        # what about spaces? or windows-style paths with C:/...
        return EventFormationUnitConfig.from_cli_str(s)
    # otherwise, allow an abbreviated format utilizing guesses
    # Expected format is now:
    #       {efu_binary}[,{calibration/file}[,{config/file}]][,{port}]
    # That is, if you specify --efu, you must give its binary path and should
    # give its port. The calibration/file determines pixel calculations, so is more
    # likely to be needed. Finally, the config file can also be supplied to change, e.g.,
    # number of pixels or rings, etc.
    parts = s.split(',')
    data = {'topic': TOPICS['event'], 'port': 9000, 'binary': ensure_executable(parts[0]),}
    data['name'] = data['binary'].stem

    if len(parts) > 1 and (len(parts) > 2 or not parts[1].isnumeric()):
        data['calibration'] = parts[1]
    else:
        data['calibration'] = guess_instr_calibration(data['name'])
    if len(parts) > 2 and (len(parts) > 3 or not parts[2].isnumeric()):
        data['config'] = parts[2]
    else:
        data['config'] = guess_instr_config(data['name'])
    if len(parts) > 1 and parts[-1].isnumeric():
        data['port'] = int(parts[-1])

    return EventFormationUnitConfig.from_dict(data)


def make_services_parser():
    from mccode_plumber import __version__
    from argparse import ArgumentParser
    parser = ArgumentParser('mp-nexus-services')
    a=parser.add_argument
    a('instrument', type=str, help='Instrument .instr or .h5 file')
    a('-v', '--version', action='version', version=__version__)
    # No need to specify the broker, or monitor source or topic names
    a('-b', '--broker', type=str, default=None, help='Kafka broker for all services', metavar='address:port')
    a('--efu', type=efu_parameter, action='append', default=None, help='Configuration of one EFU, repeatable', metavar='name,calibration,config,port')
    a('--writer-working-dir', type=str, default=None, help='Working directory for kafka-to-nexus')
    a('--writer-verbosity', type=str, default=None, help='Verbose output type (trace, debug, warning, error, critical)')
    a('--forwarder-verbosity', type=str, default=None,  help='Verbose output type (trace, debug, warning, error, critical)')
    return parser


def services():
    args = make_services_parser().parse_args()
    instr_name, instr_parameters = get_instr_name_and_parameters(args.instrument)
    kwargs = {
        'instr_name': instr_name,
        'instr_parameters': instr_parameters,
        'broker': args.broker or 'localhost:9092',
        'efu': args.efu,
        'work': args.writer_working_dir,
        'verbosity_writer': args.writer_verbosity,
        'verbosity_forwarder': args.forwarder_verbosity,
    }
    load_in_wait_load_out(**kwargs)


def load_in_wait_load_out(
        instr_name: str,
        instr_parameters: tuple[InstrumentParameter, ...],
        broker: str,
        efu: list[EventFormationUnitConfig] | None,
        work: str | None = None,
        manage: bool = True,
        verbosity_writer: str | None = None,
        verbosity_forwarder: str | None = None,
    ):
        import signal
        from time import sleep
        from colorama import Fore, Back, Style
        from mccode_plumber.manage import (
            EventFormationUnit, EPICSMailbox, Forwarder, KafkaToNexus
        )
        from mccode_plumber.manage.forwarder import forwarder_verbosity
        from mccode_plumber.manage.writer import writer_verbosity

        # Start up services if they should be managed locally
        if manage:
            if efu is None:
                data = {
                    'name': instr_name,
                    'binary': guess_instr_efu(instr_name),
                    'config': guess_instr_config(name=instr_name),
                    'calibration': guess_instr_calibration(name=instr_name),
                    'topic': TOPICS['event'],
                    'port': 9000
                }
                if any('port' in p.name for p in instr_parameters):
                    from mccode_antlr.common.expression import DataType
                    port_parameter = next(
                        p for p in instr_parameters if 'port' in p.name)
                    if port_parameter.value.has_value and port_parameter.value.data_type == DataType.int:
                        # the instrument parameter has a default, which is an integer
                        data['port'] = port_parameter.value.value
                efu = [EventFormationUnitConfig.from_dict(data)]
            things = tuple(
                EventFormationUnit.start(
                    style=Fore.BLUE, broker=broker, **x.to_dict()
                ) for x in efu) + (
                Forwarder.start(
                    name='FWD',
                    style=Fore.GREEN,
                    broker=broker,
                    config=TOPICS['config'],
                    status=TOPICS['status'],
                    verbosity=forwarder_verbosity(verbosity_forwarder),
                ),
                EPICSMailbox.start(
                    name='MBX',
                    style=Fore.YELLOW + Back.LIGHTCYAN_EX,
                    parameters=instr_parameters,
                    prefix=PREFIX,
                ),
                KafkaToNexus.start(
                    name='K2N',
                    style=Fore.RED + Style.DIM,
                    broker=broker,
                    work=work,
                    command=TOPICS['command'],
                    pool=TOPICS['pool'],
                    verbosity=writer_verbosity(verbosity_writer),
                ),
            )
        else:
            things = ()

        # Ensure stream topics exist
        register_topics(broker, list(TOPICS.values()))

        def signal_handler(signum, frame):
            if signum == signal.SIGINT:
                print('Done waiting, following SIGINT')
                for service in things:
                    service.stop()
                exit(0)
            else:
                print(f'Received signal {signum}, ignoring')

        signal.signal(signal.SIGINT, signal_handler)
        print(
            Fore.YELLOW+Back.LIGHTGREEN_EX+Style.BRIGHT
            + "\tYou can now run 'mp-nexus-splitrun' in another process"
            + " (Press CTRL+C to exit)." + Style.RESET_ALL
        )
        # signal.pause()
        while all(service.poll() for service in things):
            # Try to grab and print any updates
            sleep(0.01)
        # If we reach here, one or more service has _already_ stopped
        for service in things:
            if not service.poll():
                print(f'{service.name} exited unexpectedly')
            service.stop()


def make_splitrun_nexus_parser():
    from mccode_plumber import __version__
    from restage.splitrun import make_splitrun_parser
    parser = make_splitrun_parser()
    parser.prog = 'mp-nexus-splitrun'
    parser.add_argument('-v' ,'--version', action='version', version=__version__)
    # No need to specify the broker, or monitor source or topic names
    parser.add_argument('--structure', type=str, default=None, help='NeXus Structure JSON path')
    parser.add_argument('--structure-out', type=str, default=None, help='Output configured structure JSON path')
    parser.add_argument('--nexus-file', type=str, default=None, help='Output NeXus file path')
    return parser


def main():
    from mccode_plumber.mccode import get_mcstas_instr
    from restage.splitrun import parse_splitrun
    from mccode_plumber.splitrun import monitors_to_kafka_callback_with_arguments
    args, parameters, precision = parse_splitrun(make_splitrun_nexus_parser())
    instr = get_mcstas_instr(args.instrument)

    structure = load_file_json(args.structure if args.structure else Path(args.instrument).with_suffix('.json'))
    broker = 'localhost:9092'
    monitor_source = 'mccode-to-kafka'
    callback_topics = get_topics_json(structure)  # all structure-topics might be monitor topics?
    if len(callback_topics):
        print(f'register {callback_topics}')
        register_topics(broker, callback_topics) # ensure the topics are known to Kafka
    else:
        print('no callback topics registered')

    callback, callback_args = monitors_to_kafka_callback_with_arguments(broker, monitor_source, callback_topics)
    splitrun_kwargs = {
        'args': args, 'parameters': parameters, 'precision': precision,
        'callback': callback, 'callback_arguments': callback_args,
    }
    kwargs = {
        'nexus_file': args.nexus_file, 'structure_out': args.structure_out
    }
    for k in list(kwargs.keys()) + ['structure']:
        delattr(args, k)
    return orchestrate(instr, structure, broker, splitrun_kwargs, **kwargs)


def orchestrate(
        instr: Instr,
        structure,
        broker: str,
        splitrun_kwargs: dict,
        nexus_file: str | None= None,
        structure_out: str | None = None,
):
    from datetime import datetime, timezone
    from restage.splitrun import splitrun_args
    from mccode_plumber.forwarder import (
        forwarder_partial_streams, configure_forwarder, reset_forwarder
    )
    now = datetime.now(timezone.utc)
    title = f'{instr.name} simulation {now}: {splitrun_kwargs["args"]}'
    # kafka-to-nexus will strip off the root part of this path and put the remaining
    # location and filename under _its_ working directory.
    # Since it doesn't seem to create missing folders, we need to ensure we only
    # provide the file stem.
    filename = ensure_writable_file(nexus_file or f'{instr.name}_{now:%y%m%dT%H%M%S}.h5')

    # Tell the forwarder what to forward
    partial_streams = forwarder_partial_streams(PREFIX, TOPICS['parameter'], instr.parameters)
    forwarder_config = f"{broker}/{TOPICS['config']}"
    configure_forwarder(partial_streams, forwarder_config, PREFIX, TOPICS['parameter'])

    # Create a file-writer job
    structure = augment_structure(instr.parameters, structure, title)
    if structure_out:
        from json import dump
        with open(structure_out, 'w') as f:
            dump(structure, f)

    job_id, success = start_writer(now, structure, filename, broker, 30.0)
    if success:
        print("Writer job started -- start the simulation")
        # Do the actual simulation, calling into restage.splitrun after parsing,
        # Using the provided callbacks to send monitor data to Kafka
        splitrun_args(instr, **splitrun_kwargs)
        print("Splitrun simulation finished -- informing file-writer to stop")
    # Wait for the file-writer to finish its job (possibly kill it)
    stop_writer(broker, job_id, 20.0)
    # De-register the forwarder topics
    reset_forwarder(partial_streams, forwarder_config, PREFIX, TOPICS['parameter'])
    # Verify that the file has been written?
    # This only works if the filewriter was stared in the same directory :(
    # ensure_readable_file(filename)
    if filename.exists():
        print(f'Finished writing {filename}')
    else:
        print(f'{filename} not found, check file-writer working directory')
