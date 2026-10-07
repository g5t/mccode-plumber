"""`mp-replay` and `mp-nexus-replay`: replay a collector file someone else simulated.

The replay itself is mcstas-readout-master's and the per-point publishing is shared with
`mp-nexus-collect-replay` (tests/test_collect_replay.py). What is tested here is what the
two commands add: their arguments, where a file's McStas histograms are looked for, that a
bad structure is refused before anything starts, and waiting for the EFUs to come up.
"""
import json
import socket
import threading
from pathlib import Path

import pytest

from mccode_plumber.manage.replay import (
    REPLAY_OPTIONS, load_structure, make_replay_parser, point_directories,
)


def test_mp_replay_takes_an_instrument_a_file_and_the_replay_options():
    args = make_replay_parser().parse_args([
        'bifrost.instr.json', 'run/bifrost.h5', '--structure', 'bifrost.json',
        '--efu-senders', 'bifrost.senders.json', '--pulses-per-point', '14'])
    assert (args.instrument, args.collector, args.structure) == (
        'bifrost.instr.json', 'run/bifrost.h5', 'bifrost.json')
    assert args.pulses_per_point == 14 and args.efu_senders == 'bifrost.senders.json'
    assert all(hasattr(args, name) for name in REPLAY_OPTIONS)
    # it starts nothing, so it takes no say in how services are started
    assert not hasattr(args, 'efu')


def test_mp_nexus_replay_also_takes_the_services_options():
    args = make_replay_parser('mp-nexus-replay', services=True).parse_args([
        'bifrost.instr.json', 'run/bifrost.h5', '--writer-working-dir', 'writer',
        '--start-timeout', '30'])
    assert args.writer_working_dir == 'writer' and args.start_timeout == 30.0
    assert args.efu is None  # left to mp-nexus-services' own guess


def test_a_points_histograms_are_beside_the_file(tmp_path):
    collector = tmp_path / 'scan' / 'bifrost.h5'
    assert point_directories(collector, 3) == [tmp_path / 'scan' / str(n) for n in range(3)]


def test_a_structure_that_is_not_one_is_refused(tmp_path):
    instrument = tmp_path / 'bifrost.instr.json'
    instrument.write_text('{}')
    # the default is the instrument file with a .json suffix -- the instrument itself
    with pytest.raises(SystemExit, match='not a NeXus structure'):
        load_structure(None, instrument)
    good = tmp_path / 'bifrost.json'
    good.write_text(json.dumps({'children': []}))
    assert load_structure(good, instrument) == {'children': []}


class FakeEFU:
    """Stands in for an EventFormationUnit: a name, a command port, and whether it runs."""
    def __init__(self, port, alive=True):
        self.name, self.command, self.alive = 'efu', port, alive

    def poll(self):
        return self.alive


def _as_efu(monkeypatch):
    import mccode_plumber.manage as manage
    monkeypatch.setattr(manage, 'EventFormationUnit', FakeEFU)


def _free_port():
    with socket.socket() as s:
        s.bind(('localhost', 0))
        return s.getsockname()[1]


def test_waiting_ends_when_the_efu_answers(monkeypatch):
    from mccode_plumber.manage.orchestrate import wait_for_services
    _as_efu(monkeypatch)
    port = _free_port()
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def open_later():
        server.bind(('localhost', port))
        server.listen()

    timer = threading.Timer(0.3, open_later)
    timer.start()
    try:
        wait_for_services([FakeEFU(port)], timeout=10, sleep=lambda s: threading.Event().wait(0.1))
    finally:
        timer.cancel()
        server.close()


def test_waiting_gives_up(monkeypatch):
    from mccode_plumber.manage.orchestrate import wait_for_services
    _as_efu(monkeypatch)
    now = iter(range(0, 1000, 10))
    with pytest.raises(RuntimeError, match='did not start'):
        wait_for_services([FakeEFU(_free_port())], timeout=15,
                          sleep=lambda s: None, clock=lambda: next(now))


def test_an_efu_that_exits_is_reported(monkeypatch):
    from mccode_plumber.manage.orchestrate import wait_for_services
    _as_efu(monkeypatch)
    with pytest.raises(RuntimeError, match='exited'):
        wait_for_services([FakeEFU(_free_port(), alive=False)], timeout=5)


def test_other_services_are_not_waited_for(monkeypatch):
    from mccode_plumber.manage.orchestrate import wait_for_services
    _as_efu(monkeypatch)
    wait_for_services([object()], timeout=0)  # not an EFU: nothing to wait for
