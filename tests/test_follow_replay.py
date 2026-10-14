"""`mp-replay DIR`: follow a scan restage is still simulating, replaying points in order.

restage writes ``points.json`` before the first point, and renames each finished point's
``point.part`` to ``point``. What is tested here is the order, the waiting, refusing a
point whose primary is not on this machine, and handing each point its own publisher.
"""
import json
from pathlib import Path

import pytest

h5py = pytest.importorskip('h5py')
np = pytest.importorskip('numpy')

from mccode_plumber.manage import replay  # noqa: E402
from mccode_plumber.manage.replay import (  # noqa: E402
    PointTimeout, follow_points, is_scan_directory, make_replay_parser,
    missing_virtual_sources, replay_followed,
)


def write_point(scan: Path, point: int, source: Path | None = None) -> Path:
    """Point ``point``'s whole collector file, as restage leaves it; virtual onto ``source``."""
    ready = scan / str(point) / 'point'
    ready.mkdir(parents=True)
    path = ready / 'bifrost.h5'
    with h5py.File(path, 'w') as file:
        file.attrs['program'] = 'libreadout'
        group = file.create_group('monitor')
        group.attrs['type'] = 'Readouts'
        if source is None:
            group.create_dataset('readouts', data=np.zeros(2))
        else:
            layout = h5py.VirtualLayout(shape=(2,), dtype='f8')
            layout[:] = h5py.VirtualSource(str(source), 'records', shape=(2,))
            group.create_virtual_dataset('readouts', layout)
    return path


def start_scan(scan: Path, points: int):
    scan.mkdir(exist_ok=True)
    (scan / 'points.json').write_text(json.dumps({'points': points, 'parameters': ['a3']}))


class Clock:
    """A clock that moves only when slept on, running ``on_sleep`` each time."""

    def __init__(self, on_sleep=None):
        self.now, self.sleeps, self.on_sleep = 0.0, 0, on_sleep

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds
        self.sleeps += 1
        if self.on_sleep:
            self.on_sleep(self.sleeps)


def test_points_come_in_order_as_they_finish(tmp_path):
    scan = tmp_path / 'scan'
    # points finish 2, 0, 1 -- and the scan has not even started when following begins
    finishes = {1: lambda: start_scan(scan, 3), 2: lambda: write_point(scan, 2),
                4: lambda: write_point(scan, 0), 7: lambda: write_point(scan, 1)}
    clock = Clock(lambda n: finishes.pop(n, lambda: None)())
    got = [(point, path.parent.parent.name)
           for point, path in follow_points(scan, sleep=clock.sleep, clock=clock)]
    assert got == [(0, '0'), (1, '1'), (2, '2')]
    assert not finishes


def test_a_point_that_never_comes_is_given_up_on(tmp_path):
    scan = tmp_path / 'scan'
    start_scan(scan, 2)
    write_point(scan, 0)
    clock = Clock()
    followed = follow_points(scan, timeout=10, sleep=clock.sleep, clock=clock)
    assert next(followed)[0] == 0
    with pytest.raises(PointTimeout, match='point 1 of 2'):
        next(followed)


def test_a_point_reading_a_missing_primary_is_refused(tmp_path):
    primary = tmp_path / 'cache' / 'primary.h5'
    primary.parent.mkdir()
    with h5py.File(primary, 'w') as file:
        file.create_dataset('records', data=np.arange(2.0))
    scan = tmp_path / 'scan'
    start_scan(scan, 1)
    path = write_point(scan, 0, source=primary)
    assert missing_virtual_sources(path) == []
    primary.unlink()
    assert missing_virtual_sources(path) == [str(primary)]
    with pytest.raises(RuntimeError, match='not here'):
        list(follow_points(scan))


def test_each_point_is_replayed_with_its_own_publisher(tmp_path, monkeypatch):
    scan = tmp_path / 'scan'
    start_scan(scan, 3)
    for n in range(3):
        write_point(scan, n)
    replayed = []

    def fake_replay(filename, config, publisher, settle=0.0):
        replayed.append((Path(filename).parent.parent.name, publisher))
        return len(replayed) < 2  # the second is cancelled

    monkeypatch.setattr(replay, 'replay_file', fake_replay)
    assert replay_followed(scan, None, publisher_for=lambda n: f'publisher {n}') is False
    assert replayed == [('0', 'publisher 0'), ('1', 'publisher 1')]


def test_mp_replay_takes_a_point_timeout():
    args = make_replay_parser().parse_args(['bifrost.instr.json', 'scan', '--point-timeout', '600'])
    assert (args.collector, args.point_timeout) == ('scan', 600.0)
    assert make_replay_parser().parse_args(['i.json', 'scan']).point_timeout is None


def test_a_scan_directory_is_followed_before_it_exists(tmp_path):
    assert is_scan_directory(tmp_path / 'not_yet')
    assert is_scan_directory(tmp_path)
    assert not is_scan_directory(tmp_path / 'scan' / 'bifrost.h5')  # missing: an error, not a wait
    collector = tmp_path / 'bifrost'
    collector.write_text('')
    assert not is_scan_directory(collector)
