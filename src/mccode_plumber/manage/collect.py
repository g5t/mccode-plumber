"""Simulate first, then replay what was collected into a file-writer job.

`mp-nexus-splitrun` streams while it traces: the instrument's `ReadoutCAEN` sends events to
the EFU as rays are detected, and the file-writer job is open around the whole simulation.
An instrument whose detectors and monitors end in mcstas-readout-master `Collector*`
components instead writes its events to a collector file, and this command sends them on
afterwards:

1. restage runs the scan as `mp-nexus-splitrun` would, without publishing anything. Each
   point's McStas monitor histograms stay in its output directory, ``DIR/<point>``, and
   restage assembles one multi-point collector file per collector file name in ``DIR``.
2. With the file-writer job and the forwarder open, as `mp-nexus-splitrun` holds them,
   `readout-replay` steps through the collector file. Before each point's events are
   sent it hands over the point's parameter values, which go to the same PVs the
   simulation's pre-point hook puts them on -- the mailbox, the choppers' inputs to
   `mp-tdc`, and the simulated logs -- and the point's McStas histograms are sent.

The events are sent when they are replayed, not when they were simulated, so the
simulation can run anywhere and for as long as it needs; only the replay has to happen
while the services are up.
"""
from __future__ import annotations

from pathlib import Path


def _number(value: str):
    """A replayed parameter value, which arrives as a string, as a number if it is one."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return value


class PointPublisher:
    """A `mcstas_readout.ParameterPublisher`: each point's values to EPICS, then histograms.

    ``set_point`` is the pre-point hook `mp-nexus-splitrun` uses,
    `splitrun.parameter_pvs_callback_with_arguments`, called with the point's parameters
    as it would be before tracing that point. ``send_monitors``, if given, is called with
    the point's output directory to publish the histograms McStas left there.

    Duck-typed rather than a subclass, so this module imports without mcstas_readout.
    """

    def __init__(self, set_point, send_monitors=None, point_dirs=None):
        self.set_point = set_point
        self.send_monitors = send_monitors
        self.point_dirs = list(point_dirs or ())
        self._values: dict[int, dict] = {}

    def publish(self, point: int, name: str, value: str, unit: str | None) -> None:
        self._values.setdefault(point, {})[name] = _number(value)

    def point_ready(self, point: int) -> None:
        """All of a point's values are in: set them, then send its histograms.

        Before the point's pulse begins, so the values are in place before any event
        measured against that pulse.
        """
        self.set_point(pars=self._values.pop(point, {}))
        if self.send_monitors is not None and point < len(self.point_dirs):
            root = Path(self.point_dirs[point])
            if root.is_dir():
                self.send_monitors(root=root)

    def pulse_ready(self, point: int, pulse_ns: int) -> None:
        """Nothing to stamp: `mp-tdc` publishes the crossings on its own pulse grid."""


def replay_file(filename, config, publisher, settle: float = 0.0) -> bool:
    """Replay ``filename``, cancelling cleanly on Ctrl-C, then wait ``settle`` seconds.

    The replay runs on a worker thread: the library holds the main thread for the whole
    run and Python only sees a Ctrl-C between callbacks.

    The wait is for the events, which are still on their way when the last packet has
    gone: an EFU holds what it has formed until its producer next flushes, up to a second
    later, and the file-writer has to read it from Kafka. A writer told to stop as soon
    as the replay returns closes the file without them.
    """
    import threading
    from time import sleep
    import mcstas_readout as ro

    outcome: dict = {}
    with ro.Replay(filename, config, publisher) as job:
        def run():
            try:
                outcome['completed'] = job.run()
            except BaseException as error:  # re-raised on the main thread
                outcome['error'] = error

        worker = threading.Thread(target=run, name='readout-replay')
        worker.start()
        try:
            while worker.is_alive():
                worker.join(0.2)
        except KeyboardInterrupt:
            print('Cancelling the replay at the next point boundary')
            job.cancel()
            worker.join()
            raise
    if 'error' in outcome:
        raise outcome['error']
    if settle > 0:
        print(f'Replay done -- waiting {settle:g} s for the last events to reach the file')
        sleep(settle)
    return outcome.get('completed', False)


def choose_collector_file(directory: Path, name: str | None = None) -> Path:
    """The assembled collector file in ``directory`` to replay.

    One file holds every point's parameters and every collector group in it, so one file
    is one replay. More than one would publish each point's parameters once per file, and
    has to be asked for by ``name``.
    """
    from restage.collectors import collector_files
    found = collector_files(Path(directory))
    if name is not None:
        key = name if name.endswith('.h5') else f'{name}.h5'
        if key not in found:
            raise RuntimeError(f'No collector file {key} in {directory}; found '
                               f'{sorted(found) or "none"}')
        return found[key]
    if len(found) != 1:
        raise RuntimeError(f'Expected one collector file in {directory}, found '
                           f'{sorted(found) or "none"}; name one with --collector')
    return next(iter(found.values()))


def make_parser():
    from mccode_plumber.manage.orchestrate import make_splitrun_nexus_parser
    parser = make_splitrun_nexus_parser()
    parser.prog = 'mp-nexus-collect-replay'
    parser.description = ('Simulate a scan, then replay its collector file to the EFUs '
                          'while the file-writer records it')
    a = parser.add_argument_group('replay').add_argument
    a('--collector', type=str, default=None, metavar='NAME',
      help='Collector file to replay, when the simulation writes more than one')
    a('--counting-time', type=float, default=None, metavar='SECONDS',
      help='Send each stored readout Poisson(weight x SECONDS) times; '
           'unset sends each exactly once')
    a('--replay-seed', type=int, default=0,
      help='Seed for the replay sampling (0: non-deterministic)')
    a('--random-order', action='store_true',
      help="Shuffle each point's events before sending")
    a('--no-fold-tof', action='store_true',
      help='Send full source-to-detector times rather than folding them into the frame '
           'they arrive in')
    a('--efu-senders', type=str, default=None, metavar='JSON',
      help='readout-replay sender configuration: the EFU for each detector type')
    a('--efu-address', type=str, default='127.0.0.1',
      help='EFU address for a detector type the sender configuration does not name')
    a('--efu-port', type=int, default=9000,
      help='EFU UDP port for a detector type the sender configuration does not name')
    a('--settle', type=float, default=5.0, metavar='SECONDS',
      help='How long to keep the file open after the replay, for the events the EFUs '
           'are still producing (default 5)')
    return parser


REPLAY_ARGUMENTS = ('collector', 'counting_time', 'replay_seed', 'random_order',
                    'no_fold_tof', 'efu_senders', 'efu_address', 'efu_port', 'settle')


def replay_config(args):
    import mcstas_readout as ro
    senders = Path(args.efu_senders).read_text() if args.efu_senders else None
    return ro.ReplayConfig(counting_time=args.counting_time, seed=args.replay_seed,
                           random_order=args.random_order, senders_json=senders,
                           default_address=args.efu_address, default_port=args.efu_port,
                           fold_tof=not args.no_fold_tof)


def main():
    from datetime import datetime
    from restage.splitrun import parse_splitrun, splitrun_args
    from mccode_plumber.mccode import get_mcstas_instr
    from mccode_plumber.splitrun import (
        parameter_pvs_callback_with_arguments, monitors_to_kafka_callback_for_topics,
        require_chopper_parameters,
    )
    from mccode_plumber.manage.orchestrate import (
        PREFIX, RUN_PV, WriterUnavailable, get_chopper_specs, get_pulse_stream,
        get_simulated_logs, load_file_json, monitor_sources_and_topics, orchestrate,
        register_topics,
    )

    args, parameters, precision = parse_splitrun(make_parser())
    instr = get_mcstas_instr(args.instrument)
    structure = load_file_json(args.structure if args.structure
                               else Path(args.instrument).with_suffix('.json'))
    # Checked now, not when the writer job starts: by then the simulation has run
    if not isinstance(structure, dict) or not isinstance(structure.get('children'), list):
        raise SystemExit(f'{args.structure or "The default structure"} is not a NeXus '
                         f'structure; name one with --structure')
    # Before simulating: crossings computed from a parameter that is not there would
    # only be wrong in the file, long after the simulation that could not record it.
    choppers = get_chopper_specs(structure)
    require_chopper_parameters(instr, [c for c, _ in choppers])
    broker = args.broker or 'localhost:9092'
    replay = {name: getattr(args, name) for name in REPLAY_ARGUMENTS}
    config = replay_config(args)
    nexus_file, structure_out = args.nexus_file, args.structure_out
    for name in ('nexus_file', 'structure_out', 'broker', 'structure') + REPLAY_ARGUMENTS:
        delattr(args, name)
    # Named here rather than by restage, which would name it and keep the name to itself:
    # the replay needs to find what the simulation wrote.
    if args.dir is None:
        args.dir = f'{instr.name}{datetime.now():%Y%m%d_%H%M%S}'
    directory = Path(args.dir)

    # 1. The simulation, publishing nothing.
    splitrun_args(instr, parameters, precision, args)
    if args.dryrun:
        return
    filename = choose_collector_file(directory, replay['collector'])
    import mcstas_readout as ro
    points = ro.validate_collector_file(filename)
    print(f'Replaying {points} point(s) from {filename}')

    # 2. The replay, inside a file-writer job.
    monitor_sources, topics = monitor_sources_and_topics(structure, instr)
    register_topics(broker, topics)
    send_monitors, _ = monitors_to_kafka_callback_for_topics(
        broker=broker, sources=monitor_sources, delete_after_sending=False)
    simulated_logs = get_simulated_logs(structure, [c for c, _ in choppers])
    set_point, _ = parameter_pvs_callback_with_arguments(
        instr, [c for c, _ in choppers], RUN_PV, logs=simulated_logs, prefix=PREFIX)
    publisher = PointPublisher(set_point, send_monitors,
                               [directory.joinpath(str(n)) for n in range(points)])
    try:
        orchestrate(instr, structure, broker, {}, nexus_file=nexus_file,
                    structure_out=structure_out, choppers=choppers,
                    pulse=get_pulse_stream(structure), simulated_logs=simulated_logs,
                    run=lambda: replay_file(filename, config, publisher, replay['settle']),
                    description=f'replay of {filename}')
    except WriterUnavailable as error:
        print(error)
        raise SystemExit(1)
