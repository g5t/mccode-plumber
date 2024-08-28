from __future__ import annotations

from pathlib import Path
from typing import Union, Callable
from mccode_antlr.instr import Instr


def _is_group(x, group):
    """Is a (dict) object a (NeXus) group with the specified name?"""
    return 'name' in x and 'type' in x and x['type'] == 'group' and x['name'] == group


def _is_stream(x, name):
    """Is a (dict) object a stream with the specified name?"""
    return 'module' in x and x['module'] == name


def _make_group(name: str, nx_class: str):
    """Make a (NeXus) group dict with the specified name and class
    A group always has a name, a type, a list of children, and a list of dictionaries as attributes.
    """
    return dict(name=name, type='group',
                attributes=[dict(name='NX_class', dtype='string', values=nx_class)],
                children=[])


def _get_or_add_group(children: list, name: str, nx_class: str):
    """Get or add a group with the specified name and class to a list of children"""
    g = [x for x in children if _is_group(x, name)]
    if len(g):
        return g[0], children
    children.append(_make_group(name, nx_class))
    return children[-1], children


def _get_or_add_stream(children: list, name: str, stream_config: dict):
    """Get or add a stream with the specified name and config to a list of children"""
    m = [x for x in children if _is_stream(x, name)]
    if len(m):
        # check that the stream-config is right?
        return m[0], children
    children.append(dict(module=name, config=stream_config))
    return children[-1], children


# def a_log(ch: dict):
#     """Unused, temporarily kept for debugging. May have been correct at some point, wrong now."""
#     attrs = dict(name='NX_class', type='string', values='NXlog')
#     units = dict(name='units', type='string', values=ch.get('units', 'dimensionless'))
#     log_child = dict(module='f144', source=ch['source'], topic=ch['topic'], type=ch['dtype'], attributes=[units])
#     # log_child = dict(module='f144', source=ch['source'], topic=ch['topic'], dtype=ch['dtype'], attributes=[units])
#     desc_child = dict(module='dataset', config=dict(name='description', values=ch['description'], type='string'))
#     return dict(name=ch['name'], type='group', attributes=[attrs], children=[log_child, desc_child])


def a_log_as_of_20230626(ch: dict):
    """Correct form as of June 26, 2023. Notably, source, topic, type, and unit go in a config field.

    The ch dict must have the following keys:
        - name: the name of the logged value
        - dtype: the data type of the logged value
        - source: the Kafka source of the logged value
        - topic: the Kafka topic of the logged value
        - description: a description of the logged value
        - module: the flatbuffer module to use to log the value, e.g., 'f144'
        - unit: the unit of the logged value, e.g., 'dimensionless'

    The returned structure is:
        {name: <name>, type: 'group', attributes: [{name: 'NX_class', type: 'string', values: 'NXlog'}],
         children: [
          {module: <module>, config: {type: <dtype>, source: <source>, topic: <topic>, unit: <unit>}},
          {module: 'dataset', config: {name: 'description', type: 'string', values: <description>}}
         ]
        }
    """
    c = dict(type=ch['dtype'], topic=ch['topic'], source=ch['source'], unit=ch.get('unit', 'dimensionless'))
    # Use f144 for most things, or f143 for more general objects -- like strings
    log_child = dict(module=ch['module'], config=c)
    attrs = dict(name='NX_class', type='string', values='NXlog')
    desc_child = dict(module='dataset', config=dict(name='description', values=ch['description'], type='string'))
    return dict(name=ch['name'], type='group', attributes=[attrs], children=[log_child, desc_child])


def default_nexus_structure(instr, origin: str | None = None):
    from zenlog import log
    import moreniius.additions  # patches the Instance class to have more translation methods
    from moreniius import MorEniius
    log.info('Creating NeXus structure from instrument'
             ' -- no custom Instance to NeXus mapping is used'
             ' -- provide a JSON object, a python module and function name, or executable to use a custom mapping')
    return MorEniius.from_mccode(instr, origin=origin, only_nx=False, absolute_depends_on=True).to_nexus_structure()


def add_pvs_to_nexus_structure(ns: dict, pvs: list[dict]):
    if 'children' not in ns:
        raise RuntimeError('Top-level NeXus structure dict with toplevel list entry named "children" expected.')
    entry, ns['children'] = _get_or_add_group(ns['children'], 'entry', 'NXentry')
    # # NXlogs isn't a NeXus base class ...
    # logs, entry['children'] = _get_or_add_group(entry['children'],  'logs', 'NXlogs')
    # So dump everything directly into 'children'
    # but 'NXparameters' _does_ exist:
    parameters, entry['children'] = _get_or_add_group(entry['children'], 'parameters', 'NXparameters')
    for pv in pvs:
        if any(x not in pv for x in ['name', 'dtype', 'source', 'topic', 'description', 'module', 'unit']):
            raise RuntimeError(f"PV {pv['name']} is missing one or more required keys")
        parameters['children'].append(a_log_as_of_20230626(pv))
    return ns


def add_title_to_nexus_structure(ns: dict, title: str):
    if 'children' not in ns:
        raise RuntimeError('Top-level NeXus structure dict with toplevel list entry named "children" expected.')
    entry, ns['children'] = _get_or_add_group(ns['children'], 'entry', 'NXentry')
    entry['children'].append(dict(module='dataset', config=dict(name='title', values=title, type='string')))
    return ns


def insert_events_in_nexus_structure(ns: dict, config: dict):
    if 'children' not in ns:
        raise RuntimeError('Top-level NeXus structure dict with toplevel list entry named "children" expected.')
    entry, ns['children'] = _get_or_add_group(ns['children'], 'entry', 'NXentry')

    # check whether 'instrument' is already a group under 'entry', and add it if not
    instr, entry['children'] = _get_or_add_group(entry['children'], 'instrument', 'NXinstrument')

    # check whether 'detector' is a group under '/entry/instrument', and add it if not
    detector, instr['children'] = _get_or_add_group(instr['children'], 'detector', 'NXdetector')
    # ... TODO fill in all of the required detector elements :(

    # check whether 'events' is a group under `/entry/instrument/detector`
    events, detector['children'] = _get_or_add_group(detector['children'], 'events', 'NXevent_data')

    # Ensure that the events group has the correct stream-specification child
    # {'module': 'ev44', 'config': {'source': 'source', 'topic': 'topic'}}
    stream, events['children'] = _get_or_add_stream(events['children'], 'ev44', config)

    return ns


def get_writer_pool(broker: str = None, job: str = None, command: str = None):
    from file_writer_control import WorkerJobPool
    pool = WorkerJobPool(f"{broker}/{job}", f"{broker}/{command}")
    return pool


def define_nexus_structure(instr: Union[Path, str], pvs: list[dict], title: str = None, event_stream: dict[str, str] = None,
                           file: Union[Path, None] = None, func: Union[Callable[[Instr], dict], None] = None,
                           binary: Union[Path, None] = None, origin: str = None):
    import json
    from .mccode import get_mcstas_instr
    if file is not None and file.exists():
        with open(file, 'r') as file:
            nexus_structure = json.load(file)
    elif func is not None:
        nexus_structure = func(get_mcstas_instr(instr))
    elif binary is not None and binary.exists():
        from subprocess import run, PIPE
        result = run([binary, str(instr)], stdout=PIPE, stderr=PIPE)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to execute {binary} {instr} due to error {result.stderr.decode()}")
        nexus_structure = json.loads(result.stdout.decode())
    else:
        nexus_structure = default_nexus_structure(get_mcstas_instr(instr), origin=origin)
    nexus_structure = add_pvs_to_nexus_structure(nexus_structure, pvs)
    nexus_structure = add_title_to_nexus_structure(nexus_structure, title)
    # nexus_structure = insert_events_in_nexus_structure(nexus_structure, event_stream)
    return nexus_structure


def start_pool_writer(start_time_string, structure, filename=None, stop_time_string: str | None = None,
                      broker: str | None = None, job_topic: str | None = None, command_topic: str | None = None,
                      wait: bool = False, timeout: float | None = None, job_id: str | None = None):
    from sys import exit
    from os import EX_OK, EX_UNAVAILABLE
    from time import sleep
    from json import dumps
    from datetime import datetime, timedelta
    from file_writer_control import JobHandler, WriteJob, CommandState

    start_time = datetime.fromisoformat(start_time_string)
    if filename is None:
        filename = f'{start_time:%Y%m%d_%H%M%S}.nxs'

    pool = get_writer_pool(broker=broker, job=job_topic, command=command_topic)
    handler_opts = {'worker_finder': pool}

    handler = JobHandler(**handler_opts)
    small_string = dumps(structure, indent=None, separators=(',', ':'))

    end_time = datetime.now() if wait else None
    if stop_time_string is not None:
        end_time = datetime.fromisoformat(stop_time_string)
    print(f"write file from {start_time} until {end_time}")

    job = WriteJob(small_string, filename, broker, start_time, end_time, job_id=job_id or "")
    # start the job
    start = handler.start_job(job)
    if timeout is not None:
        try:
            # ensure the start succeeds:
            zero_time = datetime.now()
            while not start.is_done():
                if zero_time + timedelta(seconds=timeout) < datetime.now():
                    raise RuntimeError(f"Timed out while starting job {job.job_id}")
                elif start.get_state() == CommandState.ERROR:
                    raise RuntimeError(f"Starting job {job.job_id} failed with message {start.get_message()}")
                sleep(1)
        except RuntimeError as e:
            # raise RuntimeError(e.__str__() + f" The message was: {start.get_message()}")
            print(f"{e} The message was: {start.get_message()}")
            exit(EX_UNAVAILABLE)

    if wait:
        try:
            while not handler.is_done():
                sleep(1)
        except RuntimeError as error:
            print(str(error) + f'Writer failed, producing message:\n{handler.get_message}')
            exit(EX_UNAVAILABLE)
    exit(EX_OK)


def get_arg_parser():
    from argparse import ArgumentParser
    from .utils import is_callable, is_readable, is_executable, is_creatable
    parser = ArgumentParser(description="Control writing Kafka stream(s) to a NeXus file")
    a = parser.add_argument
    a('instrument', type=str, default=None, help="The mcstas instrument with EPICS PVs")
    a('-p', '--prefix', type=str, default='mcstas:')
    a('-t', '--topic', type=str, help="The Kafka broker topic to instruct the Forwarder to use")
    a('-b', '--broker', type=str, help="The Kafka broker server used by the Writer")
    a('-j', '--job', type=str, help='Writer job topic')
    a('-c', '--command', type=str, help='Writer command topic')
    a('--title', type=str, default='scan title for testing', help='Output file title parameter')
    a('--event-source', type=str)
    a('--event-topic', type=str)
    a('-f', '--filename', type=str, default=None)
    a('--ns-func', type=is_callable, default=None, help='Python module:function to produce NeXus structure')
    a('--ns-file', type=is_readable, default=None, help='Base NeXus structure, will be extended')
    a('--ns-exec', type=is_executable, default=None, help='Executable to produce NeXus structure')
    a('--ns-save', type=is_creatable, default=None, help='Path at which to save extended NeXus structure')
    a('--start-time', type=str)
    a('--stop-time',  type=str, default=None)
    a('--origin', type=str, default=None, help='component name used for the origin of the NeXus file')
    a('--wait', action='store_true', help='If provided, wait for the writer to finish before exiting')
    a('--time-out', type=float, default=120., help='Wait up to the timeout for writing to start')
    a('--job-id', type=str, default=None, help='Unique Job identifier for this write-job')

    return parser


def parameter_description(inst_param):
    desc = f"{inst_param.value.data_type} valued McStas parameter '{inst_param.name}', "
    desc += f"default: {inst_param.value}" if inst_param.value.has_value else "no default"
    if inst_param.unit is not None:
        desc += f" and expected units of {inst_param.unit}"
    return desc


def construct_writer_pv_dicts(instr: Union[Path, str], prefix: str, topic: str):
    from .mccode import get_mccode_instr_parameters
    parameters = get_mccode_instr_parameters(instr)
    return construct_writer_pv_dicts_from_parameters(parameters, prefix, topic)


def construct_writer_pv_dicts_from_parameters(parameters, prefix: str, topic: str):
    def strip_quotes(s):
        return s[1:-1] if s is not None and len(s) > 2 and (s[0] == s[-1] == '"' or s[0] == s[-1] == "'") else s
    return [dict(name=p.name, dtype=p.value.data_type.name, source=f'{prefix}{p.name}', topic=topic,
                 description=parameter_description(p), module='f144', unit=strip_quotes(p.unit)) for p in parameters]


def parse_writer_args():
    args = get_arg_parser().parse_args()
    params = construct_writer_pv_dicts(args.instrument, args.prefix, args.topic)
    structure = define_nexus_structure(args.instrument, params, title=args.title, origin=args.origin,
                                       file=args.ns_file, func=args.ns_func, binary=args.ns_exec,
                                       event_stream={'source': args.event_source, 'topic': args.event_topic})
    if args.ns_save is not None:
        from json import dump
        with open(args.ns_save, 'w') as file:
            dump(structure, file, indent=2)

    return args, params, structure


def print_time():
    from datetime import datetime
    print(datetime.now())


def start_writer():
    args, parameters, structure = parse_writer_args()
    return start_pool_writer(args.start_time, structure, args.filename, stop_time_string=args.stop_time,
                             broker=args.broker, job_topic=args.job, command_topic=args.command,
                             wait=args.wait, timeout=args.time_out, job_id=args.job_id)


def wait_on_writer():
    from sys import exit
    from os import EX_OK, EX_UNAVAILABLE
    from time import sleep
    from datetime import datetime, timedelta
    from file_writer_control import JobHandler, CommandState

    from argparse import ArgumentParser
    parser = ArgumentParser()
    a = parser.add_argument
    a('-b', '--broker', type=str, help="The Kafka broker server used by the Writer")
    a('-j', '--job', type=str, help='Writer job topic')
    a('-c', '--command', type=str, help='Writer command topic')
    a('id', type=str, help='Job id to wait on')
    a('-s', '--stop-after', type=float, help='Stop after time, seconds', default=1)
    a('-t', '--time-out', type=float, help='Time out after, seconds', default=24*60*60*30)
    args = parser.parse_args()

    pool = get_writer_pool(broker=args.broker, job=args.job, command=args.command)
    job = JobHandler(worker_finder=pool, job_id=args.id)
    stop_time = datetime.now() + timedelta(seconds=args.stop_after)
    stop = job.set_stop_time(stop_time)

    try:
        timeout = args.time_out
        zero_time = datetime.now()
        while not stop.is_done() and not job.is_done():
            if zero_time + timedelta(seconds=timeout) < datetime.now():
                print('1')
                raise RuntimeError(f"Timed out while stopping job {job.job_id}")
            elif stop.get_state() == CommandState.ERROR:
                print('2')
                raise RuntimeError(f"Stopping job {job.job_id} failed with message {stop.get_message()}")
            sleep(0.5)
    except RuntimeError as e:
        # raise RuntimeError(e.__str__() + f" The message was: {stop.get_message()}")
        exit(EX_UNAVAILABLE)
    exit(EX_OK)
