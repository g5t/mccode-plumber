from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import NamedTuple
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


#: How long to wait for the file-writer to say a job has finished after it is told to
#: stop. Closing a large file can keep it silent for well over the 15 s after which the
#: status tracker reports a job as `TIMEOUT`, so that is not taken as an answer.
STOP_TIMEOUT = 120.0


#: The consumer group kafka-to-nexus takes jobs from the job-pool topic with. Fixed in
#: kafka-to-nexus (`Command::JobListener::ConsumerGroupId`) for the pool to work at all.
WRITER_POOL_GROUP = 'kafka-to-nexus-worker-pool'

#: How long to wait for a free file-writer before refusing to submit a job. Writers
#: publish their status every 2 s.
IDLE_WAIT = 10.0


class WriterUnavailable(RuntimeError):
    """No file-writer is free to take a job."""


def _writer_pool(broker):
    """A pool watching the file-writers' command topic from now on."""
    from mccode_plumber.file_writer_control import WorkerJobPool
    return WorkerJobPool(f"{broker}/{TOPICS['pool']}", f"{broker}/{TOPICS['command']}")


def idle_writer_pool(broker, wait: float = IDLE_WAIT, clock=None, sleep=None,
                     make_pool=None):
    """A pool with a free file-writer behind it, or `WriterUnavailable` saying why not.

    A job sent to the pool when no writer is free is not refused: it waits there. The
    start then times out, and its stop goes to writers that do not have it -- but the
    job is still queued, and the next writer to come free runs it, with no stop time,
    until somebody kills it. Every job sent meanwhile queues behind it. So no job is sent
    unless a writer has said it is idle.
    """
    from datetime import datetime, timedelta
    from time import sleep as _sleep
    from mccode_plumber.file_writer_control.JobStatus import JobState
    from mccode_plumber.file_writer_control.WorkerStatus import WorkerState
    clock = clock or datetime.now
    sleep = sleep or _sleep
    pool = (make_pool or _writer_pool)(broker)
    give_up = clock() + timedelta(seconds=wait)
    while True:
        workers = pool.list_known_workers()
        if any(worker.state == WorkerState.IDLE for worker in workers):
            return pool
        if clock() >= give_up:
            break
        sleep(0.5)
    if not workers:
        raise WriterUnavailable(
            f'No file-writer has reported its status in {wait:.0f} s. Is mp-nexus-services '
            f'running, with its kafka-to-nexus, against {broker}?')
    busy = [job for job in pool.list_known_jobs() if job.state == JobState.WRITING]
    lines = [f'{worker.service_id}: {worker.state.name}' for worker in workers]
    lines += [f'job {job.job_id} ({job.file_name}) -- stop it with: mp-writer-kill '
              f'-b {broker} --topic {TOPICS["pool"]} --command {TOPICS["command"]} '
              f'{job.service_id} {job.job_id}' for job in busy]
    raise WriterUnavailable('No file-writer is free, so no job was sent:\n  '
                            + '\n  '.join(lines))


def skip_writer_pool_backlog(broker, topic: str | None = None,
                             group: str = WRITER_POOL_GROUP) -> int:
    """Move the writers' job-pool position past every job already queued; return how many.

    A writer that is busy leaves the pool, and when it comes back it takes the jobs that
    arrived meanwhile -- jobs whose senders gave up on them long ago and will never stop
    them. Done before kafka-to-nexus starts, so no writer is in the group and the position
    can be set from outside it. Nothing is deleted: the jobs are just behind the writers.
    """
    from kafka import KafkaConsumer, TopicPartition
    from kafka.structs import OffsetAndMetadata
    topic = topic or TOPICS['pool']
    consumer = KafkaConsumer(bootstrap_servers=broker, group_id=group,
                             enable_auto_commit=False)
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return 0
        tps = [TopicPartition(topic, p) for p in partitions]
        ends = consumer.end_offsets(tps)
        skipped = 0
        for tp in tps:
            committed = consumer.committed(tp)
            if committed is not None:
                skipped += max(0, ends[tp] - committed)
        consumer.commit({tp: OffsetAndMetadata(ends[tp], None, -1) for tp in tps})
        return skipped
    finally:
        consumer.close()


def wait_for_job_end(pool, job_id, timeout: float, clock=None, sleep=None):
    """Wait for the file-writer to report ``job_id`` finished, and return its last state.

    Only `DONE` and `ERROR` end the wait. `TIMEOUT` is what the status tracker calls a
    job it has heard nothing about for 15 s -- which is what a file-writer busy closing a
    large file looks like -- so it is waited through, until ``timeout`` seconds pass.
    """
    from datetime import datetime, timedelta
    from time import sleep as _sleep
    from mccode_plumber.file_writer_control.JobStatus import JobState
    clock = clock or datetime.now
    sleep = sleep or _sleep
    give_up = clock() + timedelta(seconds=timeout)
    state = pool.get_job_state(job_id)
    while state not in (JobState.DONE, JobState.ERROR) and clock() < give_up:
        sleep(1)
        state = pool.get_job_state(job_id)
    return state


def stop_writer(broker, job_id, timeout=STOP_TIMEOUT, pool=None):
    """Tell the file-writer to stop ``job_id``, and wait until it says it has.

    Pass the ``pool`` the job was started with. It has been following the command topic
    since then, so it knows the job and cannot miss the writer's answer -- which arrives
    within milliseconds of the stop. A pool made here only starts listening now, and if
    it is not listening yet when the answer comes, the job is never seen to finish.
    """
    from time import sleep
    from mccode_plumber.file_writer_control.JobStatus import JobState
    if pool is None:
        pool = _writer_pool(broker)
        sleep(1)  # give its consumer a chance to be listening before the answer comes
    pool.try_send_stop_now(None, job_id)
    state = wait_for_job_end(pool, job_id, timeout)
    if state == JobState.DONE:
        return state
    if state == JobState.ERROR:
        status = pool.get_job_status(job_id)
        print(f'The file-writer reported an error for job {job_id}: '
              f'{status.message if status else "(no message)"}')
    else:
        print(f'The file-writer has not said that job {job_id} finished, {timeout:.0f} s '
              f'after it was told to stop (last known state: {state.name}). It may still '
              f'be closing the file, or be stuck: `mp-writer-list` shows its jobs, and '
              f'`mp-writer-kill {job_id}` stops this one.')
    return state


def start_writer(start_time: datetime,
                 structure: dict,
                 filename: Path,
                 broker: str,
                 timeout: float,
                 pool=None):
    """Start a file-writer job, returning its id and the pool that started it.

    ``pool`` is the pool `idle_writer_pool` found a free writer through. The returned pool
    is `None` if the job did not start; it has already been told to stop, in case the
    writer took it up after giving up on it.
    """
    from uuid import uuid1
    from mccode_plumber.writer import writer_start
    job_id = str(uuid1())
    name = filename.name
    try:
        print(f"Starting {job_id} from {start_time} for file {name} under kafka-to-nexus' working directory")
        start, handler = writer_start(
            start_time.isoformat(), structure, filename=name,
            stop_time_string=None,
            broker=broker, job_topic=TOPICS['pool'], command_topic=TOPICS['command'],
            control_topic=TOPICS['command'], # don't switch topics
            timeout=timeout, job_id=job_id, wait=False, pool=pool,
        )
        return job_id, handler.worker_finder
    except RuntimeError as e:
        if job_id not in str(e):
            raise
        # starting the job failed, so try to kill it
        print(f"Starting {job_id} failed! Error: {e}")
        stop_writer(broker, job_id, timeout, pool=pool)
        return job_id, None


class Stream(NamedTuple):
    """One filewriter stream directive: which module, on which topic, from which source.

    The module is the part that used to be thrown away. Without it a caller asking
    "where do the monitors publish?" has to recognise the topic by name, which means
    re-deriving whatever convention the structure's author used -- and silently
    finding nothing when that convention changes. With it the question is answered by
    what the directive *is*: `da00` histograms, `ev44` events, `f144`/`tdct` logs.
    """
    module: str
    topic: str
    source: str


#: Modules carrying histogrammed monitor data, which McStas produces directly.
MONITOR_MODULES = ('da00',)
#: Modules carrying detector event data, which an EFU produces from readout packets.
EVENT_MODULES = ('ev44',)


def _walk_nodes(data):
    """Every dict in a loaded JSON object, depth first."""
    if isinstance(data, dict):
        yield data
        for value in data.values():
            yield from _walk_nodes(value)
    elif isinstance(data, (list, tuple)):
        for entry in data:
            yield from _walk_nodes(entry)


def get_stream_modules(data) -> list[Stream]:
    """Every stream directive in a NeXus structure, in the order it appears.

    A directive is a node with a string `module` whose `config` names both a topic and
    a source. Requiring the pair to sit inside a module's config -- rather than taking
    any dict that happens to carry both keys -- is what keeps a `dataset` whose values
    include 'topic' and 'source' from being mistaken for a stream. `link` modules are
    skipped by the same rule: they carry a source but no topic, because they mirror a
    group that some other module fills rather than subscribing to anything.

    Deduplicated, because one topic/source/module triple declared twice is still one
    stream, but order-preserving so that callers reporting a problem name the streams
    in the order a reader would find them.
    """
    seen, streams = set(), []
    for node in _walk_nodes(data):
        module = node.get('module')
        config = node.get('config')
        if not isinstance(module, str) or not isinstance(config, dict):
            continue
        topic, source = config.get('topic'), config.get('source')
        if not isinstance(topic, str) or not isinstance(source, str):
            continue
        entry = Stream(module, topic, source)
        if entry not in seen:
            seen.add(entry)
            streams.append(entry)
    return streams


def streams_of_module(streams: list[Stream], modules) -> list[Stream]:
    """Just the streams written by one of `modules`."""
    return [s for s in streams if s.module in modules]


def topics_of(streams: list[Stream]) -> list[str]:
    """The distinct topics `streams` publish on, in order of first appearance."""
    return list(dict.fromkeys(s.topic for s in streams))


def event_topic_from_streams(streams: list[Stream]) -> str | None:
    """The topic an EFU must publish on for the filewriter to find its events.

    `None` when the structure declares no event stream, which leaves the caller's own
    default in place -- a structure without detectors is not an error, it is an
    instrument whose monitors are the only thing being written.

    Several distinct event topics is an error rather than a choice: one EFU publishes
    to one topic, so picking any one of them would leave the others silently empty.
    An instrument that really does feed several topics needs one `--efu` per topic,
    each naming its own, which `resolve_topic` then leaves untouched.
    """
    topics = topics_of(streams_of_module(streams, EVENT_MODULES))
    if len(topics) > 1:
        raise ValueError(
            'The NeXus structure declares event streams on several topics '
            f'({", ".join(topics)}); name one per EFU with --efu ...,topic:<name>.'
        )
    return topics[0] if topics else None


def sources_by_topic(streams: list[Stream]) -> dict[str, list[str]]:
    """The sources each topic carries, in order of first appearance."""
    grouped: dict[str, list[str]] = {}
    for s in streams:
        names = grouped.setdefault(s.topic, [])
        if s.source not in names:
            names.append(s.source)
    return grouped


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


#: The attribute on an NXlog naming the instrument parameter a simulation fills it from.
#: niess writes it on every log it simulates. A structure bound to a facility's names has
#: sources that look nothing like the parameters, so this is the only way to tell which
#: value each PV should carry.
SIMULATION_PARAMETER = 'simulation_parameter'


def _attribute(node: dict, name: str):
    for attribute in node.get('attributes') or ():
        if isinstance(attribute, dict) and attribute.get('name') == name:
            return attribute.get('values')
    return None


def _log_node(group: dict, name: str) -> dict | None:
    """One named child of a group."""
    for child in group.get('children') or ():
        if isinstance(child, dict) and child.get('name') == name:
            return child
    return None


def _node_stream(node: dict | None) -> tuple[str | None, str, str | None, dict] | None:
    """The (module, source, topic, config) of the stream filling one NXlog."""
    for stream in (node or {}).get('children') or ():
        config = stream.get('config') or {}
        if 'source' in config:
            return stream.get('module'), config['source'], config.get('topic'), config
    return None


def _log_stream(group: dict, name: str) -> tuple[str | None, str, str | None] | None:
    """The (module, source, topic) of the stream filling one named NXlog child."""
    found = _node_stream(_log_node(group, name))
    return None if found is None else found[:3]


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

    A disc is simulated when its logs name the parameters that fill them
    (`simulation_parameter`), or when it has niess' old `mark_delay` log in seconds. A
    group with a `delay` and no parameters was written for a *real* run and is skipped
    rather than faked: its values are the control system's own PVs, and its crossings are
    measured by an actual pickup.

    The delay's unit is read off its log -- ESS publishes TotDly in nanoseconds -- so
    the crossings are computed from the number actually published.
    """
    from mccode_plumber.conductor import DELAY_UNITS
    specs = []
    for group in _find_groups(structure, 'NXdisk_chopper'):
        name = group.get('name')
        tdc = _module_stream(group, 'tdct')
        speed = _log_node(group, 'rotation_speed')
        if tdc is None or _node_stream(speed) is None:
            continue
        legacy = _log_node(group, 'mark_delay')
        delay = legacy if legacy is not None else _log_node(group, 'delay')
        park = _log_node(group, 'park_angle')
        simulated = legacy is not None or any(
            _attribute(node, SIMULATION_PARAMETER) for node in (speed, delay, park)
            if node is not None)
        if _node_stream(delay) is None:
            # Checked before `simulated`: a real ESS disc always has a delay, so one with
            # none is an instrument whose knob is something else.
            raise ValueError(
                f"Chopper {name!r} declares top-dead-centre times but no 'delay' log to "
                f"compute them from. An instrument whose delay knob is a phase in "
                f"degrees cannot be faked from a time; give the disc a delay parameter, "
                f"or drop its top_dead_center log."
            )
        if not simulated:
            continue            # a real chopper; nothing here to fake
        unit = _node_stream(delay)[3].get('value_units') or 's'
        if unit not in DELAY_UNITS:
            raise ValueError(f"Chopper {name!r} publishes its delay in {unit!r}, which is "
                             f"not a time unit this understands ({sorted(DELAY_UNITS)})")
        park_stream = _node_stream(park)
        specs.append((Chopper(
            name=name, tdc=tdc[0],
            speed=_node_stream(speed)[1], delay=_node_stream(delay)[1],
            park=park_stream[1] if park_stream else None,
            speed_parameter=_attribute(speed, SIMULATION_PARAMETER),
            delay_parameter=_attribute(delay, SIMULATION_PARAMETER),
            park_parameter=_attribute(park, SIMULATION_PARAMETER) if park else None,
            delay_unit=unit,
        ), tdc[1] or TOPICS['parameter']))
    return specs


#: What the per-pulse reference log is called, newest first. ECDC names the accelerator's
#: NXsource `source` and its proton current log `current`; niess before 0.8 wrote
#: `neutron_prod_info` and `current_log`.
PULSE_LOG_NAMES = ('current', 'current_log')


def get_pulse_stream(structure) -> tuple[str, str] | None:
    """Where the per-pulse reference sample goes.

    A top-dead-centre time is meaningless on its own -- it is measured *from* a pulse --
    so the instrument records its reference times as one sample per pulse in its
    `NXsource`. Everything in the file shares them.
    """
    for group in _find_groups(structure, 'NXsource'):
        for name in PULSE_LOG_NAMES:
            stream = _log_stream(group, name)
            if stream is not None:
                return stream[1], stream[2] or TOPICS['parameter']
    return None


class SimulatedLog(NamedTuple):
    """One NXlog a simulation fills from an instrument parameter, outside any chopper."""
    parameter: str
    source: str
    topic: str
    dtype: str


def get_simulated_logs(structure, choppers=()) -> list[SimulatedLog]:
    """Every log the structure says a parameter fills, except the choppers' own.

    A chopper's values are served by `mp-tdc`, which computes its crossings from them;
    everything else -- a jaw's edge, the sample rotation -- is served by the mailbox.
    One entry per source, in the order the structure gives them: one knob turning two
    frames is one PV.
    """
    excluded = {pv for chopper in choppers for pv, _ in chopper.served()}
    found, seen = [], set()
    for group in _find_groups(structure, 'NXlog'):
        parameter = _attribute(group, SIMULATION_PARAMETER)
        stream = _node_stream(group)
        if not parameter or stream is None or stream[0] != 'f144':
            continue
        module, source, topic, config = stream
        if source in excluded or source in seen:
            continue
        seen.add(source)
        found.append(SimulatedLog(parameter, source, topic or TOPICS['parameter'],
                                  config.get('dtype') or 'double'))
    return found


#: pvData type codes for the f144 dtypes a simulated log may declare.
_PV_TYPE_CODES = {'double': 'd', 'float64': 'd', 'float': 'f', 'float32': 'f',
                  'int64': 'l', 'int32': 'i', 'int': 'i', 'int16': 'h', 'int8': 'b'}


def simulated_log_strings(logs, parameters, prefix: str = PREFIX) -> list[str]:
    """Mailbox PV declarations, in `mp-epics-strings` form, for the simulated logs.

    Served under exactly the source the structure names, starting from the parameter's
    default. A log whose source is already the mailbox's own name for its parameter
    (``mcstas:<name>``) is left out: the mailbox serves that one anyway.
    """
    from mccode_plumber.splitrun import parameter_defaults
    defaults = parameter_defaults(parameters)
    out = []
    for log in logs:
        if log.source == f'{prefix}{log.parameter}':
            continue
        code = _PV_TYPE_CODES.get(log.dtype, 'd')
        default = defaults.get(log.parameter.lower(), 0.0)
        default = int(default) if code in 'bhil' else float(default)
        out.append(f'{log.source}:{code}:{default}')
    return out


def simulated_log_forwarder_streams(logs) -> list[dict]:
    """Forwarder declarations for the simulated logs, each on the topic it names."""
    return [dict(source=log.source, module='f144', topic=log.topic) for log in logs]


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
    """Traverse a loaded JSON object and return the found list of (topic, source) pairs.

    Kept for callers that only need to know which topics exist. Anything choosing
    *behaviour* from a stream wants `get_stream_modules` instead, so that it selects on
    the module rather than on the shape of a topic name.
    """
    return [(s.topic, s.source) for s in get_stream_modules(data)]


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
    # No topic: the abbreviated form does not name one, and guessing here would send
    # events to a topic the NeXus structure never mentions. `services` fills it in
    # from the structure's detector stream, or falls back to TOPICS['event'].
    data : dict[str, int | str | Path] = {
        'port': 9000, 'binary': binary, 'name': binary.stem
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
    streams = get_stream_modules(structure)
    choppers = get_chopper_specs(structure)
    kwargs = {
        'instr_name': instr_name,
        'instr_parameters': instr_parameters,
        'broker': args.broker or 'localhost:9092',
        'efu': args.efu,
        'choppers': choppers,
        'simulated_logs': get_simulated_logs(structure, [c for c, _ in choppers]),
        'pulse': get_pulse_stream(structure),
        'work': args.writer_working_dir,
        'verbosity_writer': args.writer_verbosity,
        'verbosity_forwarder': args.forwarder_verbosity,
        'event_topic': event_topic_from_streams(streams),
        'stream_topics': topics_of(streams),
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
        event_topic: str | None = None,
        stream_topics: list[str] | None = None,
        simulated_logs: list[SimulatedLog] | None = None,
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
            # Before kafka-to-nexus starts: jobs left in the pool by earlier runs would
            # otherwise be taken up by it, with no stop time and nobody to stop them.
            skipped = skip_writer_pool_backlog(broker)
            if skipped:
                print(f'Skipped {skipped} file-writer job(s) left queued in '
                      f'{TOPICS["pool"]} by earlier runs')
            if efu is None:
                data = {
                    'name': instr_name,
                    'binary': guess_instr_efu(instr_name),
                    'config': guess_instr_config(name=instr_name),
                    'calibration': guess_instr_calibration(name=instr_name),
                    # Where the structure says the detectors publish, so the filewriter
                    # is subscribed to what this EFU produces. TOPICS['event'] only
                    # survives for a structure that declares no detector at all.
                    'topic': event_topic or TOPICS['event'],
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
            # Anything given with --efu but no topic: takes the structure's, falling
            # back to TOPICS['event'] so a topic-less structure behaves as before.
            efu = [x.resolve_topic(event_topic or TOPICS['event']) for x in efu]
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
                    # the simulated logs' own sources, which the structure names outright
                    exact=simulated_log_strings(simulated_logs or (), instr_parameters),
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

        # Ensure stream topics exist. The control topics this process owns, plus every
        # topic the structure names: the structure is where the topic list actually
        # lives now, and registering only TOPICS would leave the detector and monitor
        # topics to be auto-created on first publish, without the per-topic config
        # `register_kafka_topics` applies.
        register_topics(broker, list(dict.fromkeys(
            list(TOPICS.values()) + list(stream_topics or ())
        )))

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


def monitor_sources_and_topics(structure, instr) -> tuple[dict[str, list[str]], list[str]]:
    """Which monitors publish on which topic, and every topic the structure names.

    Where each monitor publishes comes from the structure's own da00 directives, not
    from rebuilding a topic name out of the instrument name. Reconstructing it meant
    holding the same naming convention in two repositories, and disagreeing with it
    produced no error -- just an empty name list, and monitor data nobody received.
    Reading the directives also means several monitor topics work as written.
    """
    streams = get_stream_modules(structure)
    monitor_sources = sources_by_topic(streams_of_module(streams, MONITOR_MODULES))
    topics = topics_of(streams)  # ensure all topics are known to Kafka
    if not monitor_sources:
        # No da00 directive to read: keep sending every histogram to the topic this
        # has always derived, rather than sending nothing at all. A structure that
        # predates monitor streams still gets its monitors published.
        monitor_topic = f'{instr.name}_beam_monitor'
        monitor_sources = {monitor_topic: []}
        if monitor_topic not in topics:
            topics.append(monitor_topic)
    return monitor_sources, topics


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
        parameter_pvs_callback_with_arguments,
        monitors_to_kafka_callback_for_topics,
        require_chopper_parameters,
    )
    args, parameters, precision = parse_splitrun(make_splitrun_nexus_parser())
    instr = get_mcstas_instr(args.instrument)

    structure = load_file_json(args.structure if args.structure else Path(args.instrument).with_suffix('.json'))

    monitor_sources, topics = monitor_sources_and_topics(structure, instr)
    broker = args.broker or 'localhost:9092'
    register_topics(broker, topics)

    # One send per topic, each carrying the monitors the structure put on it.
    callback, callback_args = monitors_to_kafka_callback_for_topics(
        broker=broker, sources=monitor_sources
    )
    splitrun_kwargs = {
        'args': args, 'parameters': parameters, 'precision': precision,
        'callback': callback, 'callback_arguments': callback_args,
    }
    # The choppers, read from the same structure the file-writer is filling, so the PV
    # names published here are the stream sources it is waiting on.
    chopper_specs = get_chopper_specs(structure)
    require_chopper_parameters(instr, [c for c, _ in chopper_specs])
    pulse = get_pulse_stream(structure)
    # Every other log the structure says a parameter fills, served by the mailbox under
    # the source it names. Before each point they are put that point's values, as the
    # choppers' are, so a scanned sample rotation is logged as it was traced.
    simulated_logs = get_simulated_logs(structure, [c for c, _ in chopper_specs])
    # And every instrument parameter, to the mailbox PV /entry/parameters is filled from:
    # the instrument does not need an UpdateEPICS component to publish them itself.
    pre_callback, pre_callback_args = parameter_pvs_callback_with_arguments(
        instr, [c for c, _ in chopper_specs], RUN_PV, logs=simulated_logs, prefix=PREFIX
    )
    splitrun_kwargs['pre_callback'] = pre_callback
    splitrun_kwargs['pre_callback_arguments'] = pre_callback_args
    kwargs = {
        'nexus_file': args.nexus_file, 'structure_out': args.structure_out,
        'choppers': chopper_specs, 'pulse': pulse, 'simulated_logs': simulated_logs,
    }
    # restage's parser is the base of ours, so strip the arguments only this layer added
    # before the Namespace is handed back to it. Named rather than taken from `kwargs`,
    # which now also carries things that were never argparse attributes.
    for k in ('nexus_file', 'structure_out', 'broker', 'structure'):
        delattr(args, k)
    try:
        orchestrate(instr, structure, broker, splitrun_kwargs, **kwargs)
    except WriterUnavailable as error:
        print(error)
        raise SystemExit(1)


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
        simulated_logs: list[SimulatedLog] | None = None,
        run=None,
        description: str | None = None,
):
    """Hold a file-writer job and the forwarder open around one run.

    The run is ``restage.splitrun`` with ``splitrun_kwargs``, unless ``run`` is given: then
    it is called with no arguments instead, and ``description`` stands in for the
    splitrun arguments in the file's title.
    """
    from datetime import datetime, timezone
    from restage.splitrun import splitrun_args
    from mccode_plumber.forwarder import (
        forwarder_partial_streams, configure_forwarder, reset_forwarder
    )
    # Before anything else: a job sent with no free writer waits in the pool and is run,
    # unstoppable, by the next writer to come free.
    pool = idle_writer_pool(broker)
    now = datetime.now(timezone.utc)
    title = f'{instr.name} simulation {now}: {description or splitrun_kwargs["args"]}'
    # kafka-to-nexus will strip off the root part of this path and put the remaining
    # location and filename under _its_ working directory.
    # Since it doesn't seem to create missing folders, we need to ensure we only
    # provide the file stem.
    filename = ensure_writable_file(nexus_file or f'{instr.name}_{now:%y%m%dT%H%M%S}.h5')

    # Tell the forwarder what to forward. The chopper channels join the instrument
    # parameters unprefixed: those PV names came out of the structure already.
    partial_streams = forwarder_partial_streams(PREFIX, TOPICS['parameter'], instr.parameters)
    partial_streams += chopper_forwarder_streams(choppers or [], pulse)
    partial_streams += simulated_log_forwarder_streams(simulated_logs or ())
    forwarder_config = f"{broker}/{TOPICS['config']}"
    configure_forwarder(partial_streams, forwarder_config, PREFIX, TOPICS['parameter'])

    # Create a file-writer job
    structure = augment_structure(instr.parameters, structure, title)
    if structure_out:
        from json import dump
        with open(structure_out, 'w') as f:
            dump(structure, f)

    job_id, pool = start_writer(now, structure, filename, broker, 30.0, pool=pool)
    try:
        if pool is not None:
            if run is not None:
                print("Writer job started -- starting the run")
                run()
                print("Run finished -- informing file-writer to stop")
            else:
                print("Writer job started -- start the simulation")
                # Do the actual simulation, calling into restage.splitrun after parsing,
                # Using the provided callbacks to send monitor data to Kafka
                splitrun_args(instr, **splitrun_kwargs)
                print("Splitrun simulation finished -- informing file-writer to stop")
    finally:
        # Whether the simulation finished or failed. A job left running keeps the
        # writer busy, and the next run's job then times out waiting to start.
        if pool is not None:
            # Before the writer stops, so the last crossings are still inside the job.
            stop_faking_tdc(choppers)
            # Wait for the file-writer to finish its job, through the pool that started
            # it; a job that failed to start has already been told to stop
            stop_writer(broker, job_id, pool=pool)
        # De-register the forwarder topics
        reset_forwarder(partial_streams, forwarder_config, PREFIX, TOPICS['parameter'])
    # Verify that the file has been written?
    # This only works if the filewriter was stared in the same directory :(
    # ensure_readable_file(filename)
    if filename.exists():
        print(f'Finished writing {filename}')
    else:
        print(f'{filename} not found, check file-writer working directory')
