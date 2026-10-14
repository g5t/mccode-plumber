"""Replay a collector file into the data-collection services.

A simulation whose detectors and monitors end in mcstas-readout-master `Collector*`
components writes its events to a collector file instead of sending them anywhere, so it
can run on any machine, at any time, with nothing else running -- restage's `splitrun`
does exactly that. Getting the events into a NeXus file is then a separate step:

- `mp-replay` replays a collector file into services that are already running
  (`mp-nexus-services`), inside a file-writer job of its own;
- `mp-nexus-replay` starts the services, replays, and stops them again.

`mp-nexus-collect-replay` does the simulation and `mp-replay`'s part in one go.

Either command given a scan *directory* instead of a file follows the scan: restage
writes each point's whole collector files to ``DIR/<point>/point`` as soon as that point
is done, and the points are replayed in order as they appear -- while later points are
still being simulated, on this machine or on any other that shares the filesystem.

Replaying steps through the file's points. Before each point's events are sent, the
point's parameter values go to the same PVs a simulation's pre-point hook puts them on --
the mailbox, the choppers' inputs to `mp-tdc`, and the simulated logs -- and any McStas
monitor histograms restage left in the point's directory, ``<file's directory>/<point>``,
are sent. Monitors that are themselves collected need nothing of the kind: their rays are
in the file, and an EFU makes the histograms.
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



class PointTimeout(RuntimeError):
    """A followed scan point did not appear in time."""


def missing_virtual_sources(filename) -> list[str]:
    """The files `filename`'s virtual datasets read from that are not there.

    A point restage split reads its primary's records from the primary's file in the
    cache. HDF5 reads a missing source as zeros without complaint, and the replay would
    send them as events.
    """
    import h5py
    missing = set()
    with h5py.File(filename, 'r') as file:
        def visit(_, obj):
            if isinstance(obj, h5py.Dataset) and obj.is_virtual:
                missing.update(s.file_name for s in obj.virtual_sources()
                               if s.file_name != '.' and not Path(s.file_name).exists())
        file.visititems(visit)
    return sorted(missing)


def is_scan_directory(path) -> bool:
    """Whether to follow ``path`` as a scan rather than replay it as a file.

    A scan directory need not exist yet -- the follower may well start before the
    scan does -- so anything that is not a file is one, unless it is named like a
    collector file: a mistyped file name should fail now, not wait for ever.
    """
    path = Path(path)
    return not path.is_file() and path.suffix != '.h5'


def follow_points(directory, timeout: float | None = None, poll: float = 1.0,
                  sleep=None, clock=None):
    """Yield ``(point, collector file)`` for each point of a scan, in order, once it is done.

    The scan is restage's: it writes its point count to the manifest before simulating,
    and renames each point's ``point.part`` directory to ``point`` once that point's
    files are complete. Points finished out of order wait their turn. ``timeout`` bounds
    the wait for each one -- a scan that failed leaves no sign but the silence.
    """
    import json
    import time
    from restage.collectors import POINT_DIRECTORY
    from restage.splitrun import MANIFEST
    from mccode_plumber.manage.collect import choose_collector_file
    sleep = sleep or time.sleep
    clock = clock or time.monotonic
    directory = Path(directory)

    def wait_for(path: Path, what: str):
        start = last = clock()
        while not path.exists():
            now = clock()
            if timeout is not None and now - start > timeout:
                raise PointTimeout(f'Gave up waiting {timeout:g} s for {what} ({path})')
            if now - last >= 60:
                print(f'Still waiting for {what} after {now - start:.0f} s')
                last = now
            sleep(poll)

    manifest = directory / MANIFEST
    wait_for(manifest, 'the scan to start')
    points = json.loads(manifest.read_text())['points']
    for point in range(points):
        ready = directory / str(point) / POINT_DIRECTORY
        wait_for(ready, f'point {point} of {points}')
        filename = choose_collector_file(ready)
        if missing := missing_virtual_sources(filename):
            raise RuntimeError(f'{filename} reads from files that are not here: '
                               f'{", ".join(missing)}')
        yield point, filename


def replay_followed(directory, config, publisher_for, settle: float = 0.0,
                    timeout: float | None = None) -> bool:
    """Replay each point of the scan in ``directory`` as it is done; see `follow_points`.

    ``publisher_for(point)`` gives the publisher for one point's file: each file holds
    a single point, which it calls point 0.
    """
    from time import sleep
    for point, filename in follow_points(directory, timeout=timeout):
        print(f'Replaying point {point} from {filename}')
        if not replay_file(filename, config, publisher_for(point)):
            return False
    if settle > 0:
        print(f'Replay done -- waiting {settle:g} s for the last events to reach the file')
        sleep(settle)
    return True


def add_replay_arguments(parser) -> None:
    """How to replay: sampling, timing and where the EFUs are."""
    a = parser.add_argument_group('replay').add_argument
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
    a('--pulses-per-point', type=int, default=0, metavar='PULSES',
      help="Spread each point's events over PULSES source pulses, so an EFU summing that "
           'many pulses into a histogram -- a beam monitor, 14 at ESS -- publishes one per '
           'point; each point then takes PULSES / 14 s to replay (needs '
           'mcstas-readout-master with paced replay)')
    a('--settle', type=float, default=5.0, metavar='SECONDS',
      help='How long to keep the file open after the replay, for the events the EFUs '
           'are still producing (default 5)')


#: The replay options, by attribute name
REPLAY_OPTIONS = ('counting_time', 'replay_seed', 'random_order', 'no_fold_tof',
                  'efu_senders', 'efu_address', 'efu_port', 'pulses_per_point', 'settle')


def replay_config(args):
    import mcstas_readout as ro
    senders = Path(args.efu_senders).read_text() if args.efu_senders else None
    # only when asked for, so an older mcstas_readout without it still replays unpaced
    paced = {'pulses_per_point': args.pulses_per_point} if args.pulses_per_point else {}
    return ro.ReplayConfig(counting_time=args.counting_time, seed=args.replay_seed,
                           random_order=args.random_order, senders_json=senders,
                           default_address=args.efu_address, default_port=args.efu_port,
                           fold_tof=not args.no_fold_tof, **paced)



def load_structure(path, instrument):
    """The NeXus structure at ``path``, by default the instrument file with a .json suffix.

    Checked, so that a replay -- or a simulation before it -- does not run only to find
    the file-writer cannot use it.
    """
    from mccode_plumber.manage.orchestrate import load_file_json
    path = path or Path(instrument).with_suffix('.json')
    structure = load_file_json(path)
    if not isinstance(structure, dict) or not isinstance(structure.get('children'), list):
        raise SystemExit(f'{path} is not a NeXus structure; name one with --structure')
    return structure


def point_directories(filename: Path, points: int) -> list[Path]:
    """Where restage left each point's McStas output: beside the collector file, by number."""
    return [Path(filename).parent.joinpath(str(n)) for n in range(points)]


def replay_into_services(instr, structure, broker: str, filename: Path, config,
                         settle: float, nexus_file=None, structure_out=None,
                         point_timeout: float | None = None) -> None:
    """Replay ``filename`` inside a file-writer job, with the forwarder configured for it.

    A directory is a scan being simulated, followed point by point; see `follow_points`.

    The services must be running. What each point sets is what `mp-nexus-splitrun`'s
    pre-point hook would have set for it; see the module docstring.
    """
    import mcstas_readout as ro
    from mccode_plumber.splitrun import (
        parameter_pvs_callback_with_arguments, monitors_to_kafka_callback_for_topics,
        require_chopper_parameters,
    )
    from mccode_plumber.manage.orchestrate import (
        PREFIX, RUN_PV, WriterUnavailable, get_chopper_specs, get_pulse_stream,
        get_simulated_logs, monitor_sources_and_topics, orchestrate, register_topics,
    )
    choppers = get_chopper_specs(structure)
    require_chopper_parameters(instr, [c for c, _ in choppers])
    monitor_sources, topics = monitor_sources_and_topics(structure, instr)
    register_topics(broker, topics)
    send_monitors, _ = monitors_to_kafka_callback_for_topics(
        broker=broker, sources=monitor_sources, delete_after_sending=False)
    simulated_logs = get_simulated_logs(structure, [c for c, _ in choppers])
    set_point, _ = parameter_pvs_callback_with_arguments(
        instr, [c for c, _ in choppers], RUN_PV, logs=simulated_logs, prefix=PREFIX)
    if is_scan_directory(filename):
        print(f'Following the scan in {filename}')
        run = lambda: replay_followed(  # noqa: E731
            filename, config, settle=settle, timeout=point_timeout,
            publisher_for=lambda n: PointPublisher(set_point, send_monitors,
                                                   [Path(filename) / str(n)]))
    else:
        points = ro.validate_collector_file(filename)
        print(f'Replaying {points} point(s) from {filename}')
        publisher = PointPublisher(set_point, send_monitors,
                                   point_directories(filename, points))
        run = lambda: replay_file(filename, config, publisher, settle)  # noqa: E731
    try:
        orchestrate(instr, structure, broker, {}, nexus_file=nexus_file,
                    structure_out=structure_out, choppers=choppers,
                    pulse=get_pulse_stream(structure), simulated_logs=simulated_logs,
                    run=run, description=f'replay of {filename}')
    except (WriterUnavailable, PointTimeout) as error:
        print(error)
        raise SystemExit(1)


def make_replay_parser(prog: str = 'mp-replay', services: bool = False):
    from argparse import ArgumentParser
    from mccode_plumber import __version__
    parser = ArgumentParser(prog, description=(
        'Start the data-collection services, replay a collector file into a NeXus file, '
        'and stop them' if services else
        'Replay a collector file into a NeXus file, through running services'))
    a = parser.add_argument
    a('instrument', type=str, help='The instrument the file was simulated with (.instr.json)')
    a('collector', type=str,
      help='The collector file to replay, e.g. DIR/bifrost.h5; or a scan directory DIR, '
           'to replay its points in order as restage finishes them')
    a('--point-timeout', type=float, default=None, metavar='SECONDS',
      help='Following a scan directory: how long to wait for each point (default: '
           'until it appears)')
    a('-v', '--version', action='version', version=__version__)
    a('-b', '--broker', type=str, default=None, metavar='address:port',
      help='Kafka broker for the forwarder and file-writer control')
    a('--structure', type=str, default=None,
      help='NeXus Structure JSON path (default: the instrument file with a .json suffix)')
    a('--structure-out', type=str, default=None, help='Output configured structure JSON path')
    a('--nexus-file', type=str, default=None, help='Output NeXus file path')
    add_replay_arguments(parser)
    if services:
        from mccode_plumber.manage.orchestrate import add_services_arguments
        group = parser.add_argument_group('services')
        add_services_arguments(group)
        group.add_argument('--start-timeout', type=float, default=60.0, metavar='SECONDS',
                           help='How long to wait for the EFUs to start (default 60)')
    return parser


def _replay(args) -> None:
    from mccode_plumber.mccode import get_mcstas_instr
    instr = get_mcstas_instr(args.instrument)
    structure = load_structure(args.structure, args.instrument)
    replay_into_services(instr, structure, args.broker or 'localhost:9092',
                         Path(args.collector), replay_config(args), args.settle,
                         nexus_file=args.nexus_file, structure_out=args.structure_out,
                         point_timeout=args.point_timeout)


def replay():
    """`mp-replay`: replay a collector file through services that are already running."""
    _replay(make_replay_parser().parse_args())


def nexus_replay():
    """`mp-nexus-replay`: start the services, replay a collector file, stop the services."""
    from mccode_plumber.manage.orchestrate import (
        SERVICES_ARGUMENTS, services_kwargs, start_services, stop_services,
        wait_for_services,
    )
    args = make_replay_parser('mp-nexus-replay', services=True).parse_args()
    load_structure(args.structure, args.instrument)  # before anything is started
    things = start_services(**services_kwargs(
        args.instrument, args.structure, args.broker,
        **{name: getattr(args, name) for name in SERVICES_ARGUMENTS}))
    try:
        wait_for_services(things, timeout=args.start_timeout)
        _replay(args)
    finally:
        stop_services(things)
