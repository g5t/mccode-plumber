def test_monitor_streams():
    """BIFROST's beam monitors, as niess writes them, are found with their stream."""
    from mccode_plumber.manage.orchestrate import get_stream_pairs
    from niess.bifrost import Primary
    from niess.instrument import Instrument, Mount
    from niess.nexus import to_nexus_structure
    from niess.nexus.bifrost import BIFROST_REGISTRY

    instrument = Instrument(name='bifrost', origin='sample_origin', parts=(
        Mount(name='primary', content=Primary.from_calibration()),))
    streams = get_stream_pairs(to_nexus_structure(instrument, registry=BIFROST_REGISTRY))
    monitors = {source for topic, source in streams if topic == 'bifrost_beam_monitor'}

    assert monitors == {f'{x}_monitor' for x in ['psc', 'overlap', 'bandwidth', 'normalization']}


if __name__ == '__main__':
    test_monitor_streams()
