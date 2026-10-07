"""Simulate first, then replay what was collected into a file-writer job.

`mp-nexus-splitrun` streams while it traces: the instrument's `ReadoutCAEN` sends events to
the EFU as rays are detected, and the file-writer job is open around the whole simulation.
An instrument whose detectors and monitors end in mcstas-readout-master `Collector*`
components instead writes its events to a collector file, and this command sends them on
afterwards, in one go:

1. restage runs the scan as `mp-nexus-splitrun` would, without publishing anything, and
   assembles one multi-point collector file per collector file name in ``DIR``.
2. The file is replayed as `mp-replay` replays one, into the running services.

The two steps can also be taken apart: restage's `splitrun` simulates with nothing else
running -- on a cluster, say -- and `mp-replay` or `mp-nexus-replay` replays the file
later. See :mod:`mccode_plumber.manage.replay`.
"""
from __future__ import annotations

from pathlib import Path

# the replay's pieces, from where they live
from mccode_plumber.manage.replay import (  # noqa: F401
    PointPublisher, REPLAY_OPTIONS, add_replay_arguments, load_structure, replay_config,
    replay_file, replay_into_services,
)


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
    parser.add_argument('--collector', type=str, default=None, metavar='NAME',
                        help='Collector file to replay, when the simulation writes more '
                             'than one')
    add_replay_arguments(parser)
    return parser


REPLAY_ARGUMENTS = ('collector',) + REPLAY_OPTIONS


def main():
    from datetime import datetime
    from restage.splitrun import parse_splitrun, splitrun_args
    from mccode_plumber.mccode import get_mcstas_instr
    from mccode_plumber.splitrun import require_chopper_parameters
    from mccode_plumber.manage.orchestrate import get_chopper_specs

    args, parameters, precision = parse_splitrun(make_parser())
    instr = get_mcstas_instr(args.instrument)
    # Checked now, not when the writer job starts: by then the simulation has run
    structure = load_structure(args.structure, args.instrument)
    # Before simulating: crossings computed from a parameter that is not there would
    # only be wrong in the file, long after the simulation that could not record it.
    require_chopper_parameters(instr, [c for c, _ in get_chopper_specs(structure)])
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

    # 1. The simulation, publishing nothing.
    splitrun_args(instr, parameters, precision, args)
    if args.dryrun:
        return
    # 2. The replay, inside a file-writer job.
    filename = choose_collector_file(Path(args.dir), replay['collector'])
    replay_into_services(instr, structure, broker, filename, config, replay['settle'],
                         nexus_file=nexus_file, structure_out=structure_out)
