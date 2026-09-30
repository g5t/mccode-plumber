"""Regenerate `bifrost_structure_excerpts.json` from niess.

Not a test and not run by one: it needs niess, which mccode-plumber does not depend on.
Run it with a niess that writes BIFROST under ECDC's names (0.8, or the
`adopt-ecdc-bindings` branch) when the structure niess writes changes.

    python tests/data/make_bifrost_structure_excerpts.py
"""
import json
from pathlib import Path

#: Enough of the instrument to meet every kind of stream plumber has to handle: the pulse
#: reference, a disc turning each way, bound and unbound jaws, both motorised angles, and
#: one detector.
KEEP = ('name', 'source', 'pulse_shaping_chopper_1', 'bandwidth_chopper_2',
        'divergence_slit_1', 'mask', 'sample_jaws', 'sample_rotation',
        'detector_tank_angle', 'channel_1_1_triplet')


def excerpt(streams):
    from niess.bifrost.bifrost import instrument
    from niess.nexus import to_nexus_structure
    from niess.nexus.bifrost import BIFROST_REGISTRY
    structure = to_nexus_structure(instrument(), registry=BIFROST_REGISTRY, streams=streams)
    group = structure['children'][0]['children'][0]
    kept = [c for c in group['children']
            if (c.get('name') or c.get('config', {}).get('name')) in KEEP]
    for child in kept:
        if child.get('name') == 'channel_1_1_triplet':
            # the pixel datasets are large and nothing here reads them
            child['children'] = [x for x in child['children']
                                 if x.get('type') == 'group' and x['name'] == 'data']
    group['children'] = kept
    return structure


def main():
    from niess.bifrost.ecdc import bifrost_streams
    out = {
        '_comment': 'BIFROST as niess writes it, cut down; see '
                    'make_bifrost_structure_excerpts.py. `ecdc` is bifrost_streams(), a '
                    'simulation named as ECDC binds the real instrument; `simulated` is '
                    'the plain SIMULATED binder.',
        'ecdc': excerpt(bifrost_streams()),
        'simulated': excerpt(None),
    }
    path = Path(__file__).with_name('bifrost_structure_excerpts.json')
    path.write_text(json.dumps(out, indent=1) + '\n')


if __name__ == '__main__':
    main()
