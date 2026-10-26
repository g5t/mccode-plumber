import h5py
import numpy as np
import pytest


def _written(path):
    """A file laid out as kafka-to-nexus leaves one: extensible datasets in big chunks."""
    with h5py.File(path, 'w', libver='v110') as f:
        f.attrs['file_name'] = 'written.h5'
        events = f.create_group('entry/instrument/triplet/data')
        events.attrs['NX_class'] = 'NXevent_data'
        for name, values in (('event_id', np.arange(5000, dtype='int32')),
                             ('event_time_zero', np.arange(30, dtype='uint64'))):
            dataset = events.create_dataset(name, data=values, maxshape=(None,),
                                            chunks=(1 << 20,))
            dataset.attrs['units'] = np.bytes_(b'ns')
        signal = f.create_dataset('entry/monitor/signal', data=np.ones((7, 3), 'uint32'),
                                  maxshape=(None, 3), chunks=(1 << 18, 3))
        signal.attrs.create('axes', 'time:frame_time', dtype=h5py.string_dtype())
        log = f.create_dataset('entry/log/alarm_message', shape=(0,), maxshape=(None,),
                               chunks=(1024,), dtype=h5py.string_dtype())
        log.attrs['empty'] = h5py.Empty('f8')
        f['entry/title'] = np.bytes_(b'a scalar, unchunked')
        f['entry/data'] = h5py.SoftLink('/entry/instrument/triplet/data')
        f['entry/event_id'] = events['event_id']  # a second hard link


def _contents(path):
    out = {}
    with h5py.File(path, 'r') as f:
        def visit(name, obj):
            attrs = {k: (obj.attrs.get_id(k).dtype, repr(obj.attrs[k])) for k in obj.attrs}
            data = obj[()].tolist() if isinstance(obj, h5py.Dataset) else None
            out[name] = (type(obj).__name__, attrs, data,
                         getattr(obj, 'dtype', None), getattr(obj, 'shape', None))
        f.visititems(visit)
        out['/'] = dict(f.attrs)
        out['links'] = (f.get('entry/data', getlink=True).path,
                        f['entry/event_id'] == f['entry/instrument/triplet/data/event_id'])
    return out


def test_compact_nexus_keeps_contents(tmp_path):
    from mccode_plumber.manage.orchestrate import compact_nexus
    path = tmp_path / 'written.h5'
    _written(path)
    before, size = _contents(path), path.stat().st_size

    compact_nexus(path)

    assert _contents(path) == before
    assert path.stat().st_size < size / 10
    assert not list(tmp_path.glob('.*.compact'))
    with h5py.File(path, 'r', swmr=True) as f:
        assert f['entry/instrument/triplet/data/event_id'].chunks is None
        assert f['entry/monitor/signal'].chunks is None


def test_compact_nexus_failure_keeps_original(tmp_path, monkeypatch):
    from mccode_plumber.manage import orchestrate
    path = tmp_path / 'written.h5'
    _written(path)
    original = path.read_bytes()

    def fail(*args, **kwargs):
        raise OSError('disk full')
    monkeypatch.setattr(h5py.Group, 'create_dataset', fail)
    orchestrate.compact_nexus(path)

    assert path.read_bytes() == original
    assert not list(tmp_path.glob('.*.compact'))


@pytest.mark.parametrize('flag, expected', [([], True), (['--no-compact'], False)])
def test_no_compact_flag(flag, expected):
    from mccode_plumber.manage.replay import make_replay_parser
    args = make_replay_parser().parse_args(['instr.json', 'collector.h5'] + flag)
    assert args.compact is expected
