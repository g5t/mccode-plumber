from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from mccode_antlr.common import InstrumentParameter
from mccode_plumber.conductor import Chopper
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

#: The PV `mp-tdc` watches to know whether a run is in progress. Not prefixed: it lives in
#: the same namespace as the chopper channels rather than with the McStas parameters, and
#: is the one thing `mp-nexus-splitrun` and the services process have to agree on by name.
RUN_PV = 'tdc_run'


def guess_instr_config(name: int | str | Path) -> Path:
    if isinstance(name, int):
        raise ValueError('EFU parameter parsing error passed integer value to guess function')
    if isinstance(name, Path):
        name = name.stem
    guess = f'/event-formation-unit/configs/{name}/configs/{name}.json'
    return ensure_readable_file(Path(guess))


def guess_instr_calibration(name: int | str | Path) -> Path:
    if isinstance(name, int):
        raise ValueError('EFU parameter parsing error passed integer value to guess function')
    if isinstance(name, Path):
        name = name.stem
    guess = f'/event-formation-unit/configs/{name}/configs/{name}nullcalib.json'
    return ensure_readable_file(Path(guess))


def guess_instr_efu(name: str) -> Path:
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


def get_stream_pairs_list(data: list | tuple):
    topics = set()
    for entry in data:
        if isinstance(entry, dict):
            topics.update(get_stream_pairs_dict(entry))
        elif isinstance(entry, (list, tuple)):
            topics.update(get_stream_pairs_list(entry))
    return topics


def get_stream_pairs_dict(data: dict):
    topics = set()
    if all(k in data for k in ('topic', 'source')):
        topics.add((data['topic'], data['source']))
    for k, v in data.items():
        if isinstance(v, dict):
            topics.update(get_stream_pairs_dict(v))
        elif isinstance(v, (list, tuple)):
            topics.update(get_stream_pairs_list(list(v)))
    return topics


def _nx_class(node) -> str | None:
    for attribute in node.get('attributes') or ():
        if isinstance(attribute, dict) and attribute.get('name') == 'NX_class':
            return attribute.get('values')
    return None


def _find_groups(data, nx_class: str, out: list | None = None) -> list[dict]:
    """Every group of one NeXus class anywhere in a structure."""
    out = [] if out is None else out
    if isinstance(data, dict):
        if _nx_class(data) == nx_class:
            out.append(data)
        for value in data.values():
            _find_groups(value, nx_class, out)
    elif isinstance(data, (list, tuple)):
        for value in data:
            _find_groups(value, nx_class, out)
    return out


def _log_stream(group: dict, name: str) -> tuple[str | None, str, str | None] | None:
    """The (module, source, topic) of the stream filling one named NXlog child."""
    for child in group.get('children') or ():
        if isinstance(child, dict) and child.get('name') == name:
            for stream in child.get('children') or ():
                config = stream.get('config') or {}
                if 'source' in config:
                    return stream.get('module'), config['source'], config.get('topic')
    return None


def _module_stream(group: dict, module: str) -> tuple[str | None, str | None] | None:
    """The (source, topic) of the first child filled by one stream module."""
    for child in group.get('children') or ():
        if not isinstance(child, dict):
            continue
        for stream in child.get('children') or ():
            if stream.get('module') == module:
                config = stream.get('config') or {}
                return config.get('source'), config.get('topic')
    return None


def get_chopper_specs(structure) -> list[tuple[Chopper, str]]:
    """The discs a run has to publish top-dead-centre times for, and their topics.

    The structure is the single source of truth on purpose. It already says, for every
    `NXdisk_chopper`, which channel the crossings arrive on and which parameters they
    follow from -- so reading them from here is what guarantees the PVs served are the
    same names the file-writer is waiting on.

    A group written for a *real* run is skipped rather than faked: its speed and delay are
    the control system's own PVs, and its crossings are measured by an actual pickup.
    ESS spells that group's electronic delay `delay`; niess deliberately spells the McStas
    one `mark_delay`, because they are different quantities.
    """
    specs = []
    for group in _find_groups(structure, 'NXdisk_chopper'):
        name = group.get('name')
        tdc = _module_stream(group, 'tdct')
        speed = _log_stream(group, 'rotation_speed')
        delay = _log_stream(group, 'mark_delay')
        if tdc is None or speed is None:
            continue
        if delay is None:
            if _log_stream(group, 'delay') is not None:
                continue        # a real chopper; nothing here to fake
            raise ValueError(
                f"Chopper {name!r} declares top-dead-centre times but no 'mark_delay' "
                f"log to compute them from. An instrument whose delay knob is a phase "
                f"in degrees cannot be faked from seconds; give the disc a delay "
                f"parameter, or drop its top_dead_center log."
            )
        park = _log_stream(group, 'park_angle')
        specs.append((Chopper(name=name, tdc=tdc[0], speed=speed[1], delay=delay[1],
                              park=park[1] if park else None),
                      tdc[1] or TOPICS['parameter']))
    return specs


def get_pulse_stream(structure) -> tuple[str, str] | None:
    """Where the per-pulse reference sample goes.

    A top-dead-centre time is meaningless on its own -- it is measured *from* a pulse --
    so the instrument records its reference times as one sample per pulse under
    `neutron_prod_info`. Everything in the file shares them.
    """
    for group in _find_groups(structure, 'NXsource'):
        stream = _log_stream(group, 'current_log')
        if stream is not None:
            return stream[1], stream[2] or TOPICS['parameter']
    return None


def chopper_forwarder_streams(specs, pulse) -> list[dict]:
    """Forwarder declarations for the PVs `mp-tdc` serves.

    `tdct` for the crossings, `f144` for everything else, and no prefix: these PV names
    come from the structure already and are not McStas parameter names.
    """
    from mccode_plumber.forwarder import chopper_partial_streams
    partial = []
    for chopper, topic in specs:
        values = [n for n in (chopper.speed, chopper.delay, chopper.park) if n]
        partial += chopper_partial_streams([dict(tdc=chopper.tdc, values=values)], topic)
    if pulse is not None:
        partial.append(dict(source=pulse[0], module='f144', topic=pulse[1]))
    return partial


def get_stream_pairs(data: dict) -> list[tuple[str, str]]:
    """Traverse a loaded JSON object and return the found list of (topic, source) pairs."""
    return list(get_stream_pairs_dict(data))


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
    binary: Path = ensure_executable(parts[0])
    data : dict[str, int | str | Path] = {
        'topic': TOPICS['event'], 'port': 9000, 'binary': binary, 'name': binary.stem
    }

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
    a('--structure', type=str, default=None, help='NeXus Structure JSON path, read to find the choppers')
    return parser


def services():
    args = make_services_parser().parse_args()
    instr_name, instr_parameters = get_instr_name_and_parameters(args.instrument)
    # The same default `mp-nexus-splitrun` uses, so both processes read one description of
    # the choppers and cannot disagree about the PV names.
    structure_path = Path(args.structure or Path(args.instrument).with_suffix('.json'))
    structure = load_file_json(structure_path) if structure_path.exists() else {}
    kwargs = {
        'instr_name': instr_name,
        'instr_parameters': instr_parameters,
        'broker': args.broker or 'localhost:9092',
        'efu': args.efu,
        'choppers': get_chopper_specs(structure),
        'pulse': get_pulse_stream(structure),
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
        choppers: list | None = None,
        pulse: tuple[str, str] | None = None,
        work: str | None = None,
        manage: bool = True,
        verbosity_writer: str | None = None,
        verbosity_forwarder: str | None = None,
    ):
        import signal
        from time import sleep
        from colorama import Fore, Back, Style
        from mccode_plumber.manage import (
            EventFormationUnit, EPICSMailbox, Forwarder, KafkaToNexus, TDCFaker
        )
        from mccode_plumber.manage.forwarder import forwarder_verbosity
        from mccode_plumber.manage.writer import writer_verbosity
        from mccode_plumber.manage.manager import Triage

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
                    style=Fore.BLUE,
                    broker=broker,
                    triage=Triage(ignore=["graphite", ":2003 failed"]),
                    **x.to_dict()
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
                    triage=Triage(ignore=["ignored by this consumer instance"]),
                    broker=broker,
                    work=work,
                    command=TOPICS['command'],
                    pool=TOPICS['pool'],
                    verbosity=writer_verbosity(verbosity_writer),
                ),
            )
            # Only for an instrument that actually has discs: a server with nothing to
            # publish is a process to shut down later for no reason.
            if choppers:
                things += (
                    TDCFaker.start(
                        name='TDC',
                        style=Fore.MAGENTA,
                        choppers=tuple(c for c, _ in choppers),
                        pulse_pv=pulse[0] if pulse else 'pulse',
                        run_pv=RUN_PV,
                    ),
                )
            longest_name = max(len(thing.name) for thing in things)
            for thing in things:
                thing.name_padding = longest_name - len(thing.name)
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
    # No need to specify the monitor source or topic names
    parser.add_argument(
        '-b', '--broker', type=str, default=None, metavar='address:port',
        help='Kafka broker for monitor data, EPICS forwarding and filewriter control',
    )
    parser.add_argument('--structure', type=str, default=None, help='NeXus Structure JSON path')
    parser.add_argument('--structure-out', type=str, default=None, help='Output configured structure JSON path')
    parser.add_argument('--nexus-file', type=str, default=None, help='Output NeXus file path')
    return parser


def main():
    from mccode_plumber.mccode import get_mcstas_instr
    from restage.splitrun import parse_splitrun
    from mccode_plumber.splitrun import (
        chopper_parameters_callback_with_arguments,
        monitors_to_kafka_callback_with_arguments,
    )
    args, parameters, precision = parse_splitrun(make_splitrun_nexus_parser())
    instr = get_mcstas_instr(args.instrument)

    structure = load_file_json(args.structure if args.structure else Path(args.instrument).with_suffix('.json'))

    streams = get_stream_pairs(structure)
    # All monitors should use a single topic:
    monitor_topic = f'{instr.name}_beam_monitor'
    monitor_names = [s[1] for s in streams if s[0] == monitor_topic]

    broker = args.broker or 'localhost:9092'
    topics = list({s[0] for s in streams}) # ensure all topics are known to Kafka
    register_topics(broker, topics)

    # Configure the callback to send monitor data to Kafka, using the common topic with source names as monitor names
    callback, callback_args = monitors_to_kafka_callback_with_arguments(
        broker=broker, topic=monitor_topic, source=None, names=monitor_names
    )
    splitrun_kwargs = {
        'args': args, 'parameters': parameters, 'precision': precision,
        'callback': callback, 'callback_arguments': callback_args,
    }
    # The choppers, read from the same structure the file-writer is filling, so the PV
    # names published here are the stream sources it is waiting on.
    chopper_specs = get_chopper_specs(structure)
    pulse = get_pulse_stream(structure)
    if chopper_specs:
        pre_callback, pre_callback_args = chopper_parameters_callback_with_arguments(
            instr, [c for c, _ in chopper_specs], RUN_PV
        )
        splitrun_kwargs['pre_callback'] = pre_callback
        splitrun_kwargs['pre_callback_arguments'] = pre_callback_args
    kwargs = {
        'nexus_file': args.nexus_file, 'structure_out': args.structure_out,
        'choppers': chopper_specs, 'pulse': pulse,
    }
    # restage's parser is the base of ours, so strip the arguments only this layer added
    # before the Namespace is handed back to it. Named rather than taken from `kwargs`,
    # which now also carries things that were never argparse attributes.
    for k in ('nexus_file', 'structure_out', 'broker', 'structure'):
        delattr(args, k)
    orchestrate(instr, structure, broker, splitrun_kwargs, **kwargs)


def stop_faking_tdc(choppers) -> None:
    """Tell `mp-tdc` the run is over.

    Best effort: a services process that was never started, or one already gone, is not a
    reason to fail a simulation that has already finished and been written.
    """
    if not choppers:
        return
    try:
        from p4p.client.thread import Context
        context = Context('pva')
        context.put(RUN_PV, 0)
        context.close()
    except Exception as error:
        print(f'warning: could not stop top-dead-centre publishing ({error})')


def orchestrate(
        instr: Instr,
        structure,
        broker: str,
        splitrun_kwargs: dict,
        nexus_file: str | None= None,
        structure_out: str | None = None,
        choppers: list | None = None,
        pulse: tuple[str, str] | None = None,
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

    # Tell the forwarder what to forward. The chopper channels join the instrument
    # parameters unprefixed: those PV names came out of the structure already.
    partial_streams = forwarder_partial_streams(PREFIX, TOPICS['parameter'], instr.parameters)
    partial_streams += chopper_forwarder_streams(choppers or [], pulse)
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
        # Before the writer stops, so the last crossings are still inside the job.
        stop_faking_tdc(choppers)
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
