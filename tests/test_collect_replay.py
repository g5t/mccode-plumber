"""`mp-nexus-collect-replay`: simulate first, then replay the collector file.

The replay itself is mcstas-readout-master's; what is tested here is what this command
adds around it -- that each replayed point reaches the PVs and Kafka as a simulated point
does in `mp-nexus-splitrun`, and that the right collector file is chosen.
"""
from pathlib import Path

import pytest

from mccode_plumber.manage.collect import (
    PointPublisher, REPLAY_ARGUMENTS, choose_collector_file, make_parser,
)


class Recorder:
    def __init__(self):
        self.calls = []

    def set_point(self, pars):
        self.calls.append(('set', dict(pars)))

    def send(self, root):
        self.calls.append(('send', Path(root).name))


def replay_points(publisher, points):
    """Drive a publisher as `readout-replay` does: publish in name order, then ready."""
    for point, values in enumerate(points):
        for name in sorted(values):
            publisher.publish(point, name, values[name], None)
        publisher.point_ready(point)
        publisher.pulse_ready(point, 1_000_000_000 * (point + 1))


def test_each_point_is_set_before_its_histograms_are_sent(tmp_path):
    for n in range(2):
        tmp_path.joinpath(str(n)).mkdir()
    record = Recorder()
    publisher = PointPublisher(record.set_point, record.send,
                               [tmp_path / '0', tmp_path / '1'])
    replay_points(publisher, [{'a3': '0', 'ei': '4.5'}, {'a3': '1.5', 'ei': '4.5'}])
    assert record.calls == [
        ('set', {'a3': 0.0, 'ei': 4.5}), ('send', '0'),
        ('set', {'a3': 1.5, 'ei': 4.5}), ('send', '1'),
    ]


def test_values_that_are_not_numbers_pass_through():
    record = Recorder()
    PointPublisher(record.set_point).point_ready(0)
    publisher = PointPublisher(record.set_point)
    replay_points(publisher, [{'mode': 'fast', 'n': '3'}])
    assert record.calls[-1] == ('set', {'mode': 'fast', 'n': 3.0})


def test_a_point_without_its_directory_sends_no_histograms(tmp_path):
    record = Recorder()
    publisher = PointPublisher(record.set_point, record.send, [tmp_path / 'gone'])
    replay_points(publisher, [{'a3': '0'}])
    assert record.calls == [('set', {'a3': 0.0})]


def _collector_file(path: Path):
    h5py = pytest.importorskip('h5py')
    with h5py.File(path, 'w') as file:
        file.attrs['program'] = 'libreadout'
    return path


def test_the_one_collector_file_is_chosen(tmp_path):
    pytest.importorskip('restage.collectors')
    only = _collector_file(tmp_path / 'bifrost.h5')
    tmp_path.joinpath('other.h5').write_bytes(b'not hdf5')
    assert choose_collector_file(tmp_path) == only


def test_several_collector_files_must_be_named(tmp_path):
    pytest.importorskip('restage.collectors')
    _collector_file(tmp_path / 'bifrost.h5')
    monitors = _collector_file(tmp_path / 'monitors.h5')
    with pytest.raises(RuntimeError, match='--collector'):
        choose_collector_file(tmp_path)
    assert choose_collector_file(tmp_path, 'monitors') == monitors
    with pytest.raises(RuntimeError, match='No collector file'):
        choose_collector_file(tmp_path, 'detectors')


def parse(argv):
    """As restage's `parse_splitrun` does: scan parameters are moved ahead of the options
    first, which argparse before Python 3.12 needs to take them after options."""
    from mccode_antlr.run.runner import sort_args
    return make_parser().parse_args(sort_args(argv))


def test_the_parser_takes_splitrun_and_replay_arguments():
    args = parse([
        'bifrost.instr.json', '-n', '1M', '--counting-time', '2', '--no-fold-tof',
        '--efu-port', '9001', 'sample_rotation=0:10'])
    assert args.counting_time == 2.0 and args.no_fold_tof and args.efu_port == 9001
    assert args.pulses_per_point == 0
    assert make_parser().parse_args(
        ['bifrost.instr.json', '--pulses-per-point', '14']).pulses_per_point == 14
    assert all(hasattr(args, name) for name in REPLAY_ARGUMENTS)
