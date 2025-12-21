from mccode_antlr.instr import Instr


def get_registries():
    from mccode_antlr.reader import GitHubRegistry

    registries = ['mcstas-chopper-lib', 'mcstas-detector-tubes', 'mcstas-frame-tof-monitor', 'mccode-mcpl-filter',]
    registries = [GitHubRegistry(
        name,
        url=f'https://github.com/mcdotstar/{name}',
        filename='pooch-registry.txt',
        version='main'
    ) for name in registries]

    return registries


def instr_to_nexus_structure_json(instrument: Instr):
    from tempfile import TemporaryDirectory
    from pathlib import Path
    from json import load
    import moreniius
    import moreniius.additions

    moreniius.additions.BIFROST_DETECTOR_TOPIC='SimulatedEvents'

    nx = moreniius.MorEniius.from_mccode(
        instrument,
        origin='sample_origin',
        only_nx=False,
        absolute_depends_on=False,
    )

    with TemporaryDirectory() as tmpdir:
        json_file = Path(tmpdir)/f'{instrument.name}.json'
        nx.to_json(json_file.as_posix())
        with open(json_file, 'r') as f:
            return load(f)


def test_monitor_streams():
    from mccode_antlr import Flavor
    from mccode_plumber.manage.orchestrate import get_stream_pairs
    from mccode_antlr.assembler import Assembler
    from niess.bifrost import Primary
    from scipp import scalar

    assembler = Assembler("bifrost", flavor=Flavor.MCSTAS, registries=get_registries())

    primary = Primary.from_calibration()
    primary.source.n_pulses = 1
    primary.source.accelerator_power = scalar(2.0, unit='MW')

    primary.to_mccode(assembler)

    instr = instr_to_nexus_structure_json(assembler.instrument)
    streams = get_stream_pairs(instr)
    topics = set(t for t, _ in streams)
    sources = set(n for _, n in streams)

    assert topics == {'bifrost_beam_monitor'}
    assert sources == {f'{x}_monitor' for x in ['psc', 'overlap', 'bandwidth', 'normalization']}


if __name__ == '__main__':
    test_monitor_streams()
