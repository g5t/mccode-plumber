"""Parameters the structure already logs are not logged a second time."""
import pytest
from mccode_plumber.writer import (
    add_pvs_to_nexus_structure, logged_names, GENERIC_LOG_NAMES,
)


def pv(name, source=None):
    return dict(name=name, dtype='double', source=source or f'mcstas:{name}',
                topic='SimulatedParameters', description=f'{name} description',
                module='f144', unit='degree')


def nxlog(name, source, topic='bifrost_motors'):
    """The shape niess' Motor.to_log emits: an NXlog named for the parameter."""
    return {'name': name, 'type': 'group',
            'attributes': [{'name': 'NX_class', 'dtype': 'string', 'values': 'NXlog'}],
            'children': [{'module': 'f144', 'config': {'source': source, 'topic': topic}}]}


def nxpositioner(name, source, topic='bifrost_motors'):
    """The shape niess' Motor.to_positioner emits: the log is a child named 'value'."""
    return {'name': name, 'type': 'group',
            'attributes': [{'name': 'NX_class', 'dtype': 'string', 'values': 'NXpositioner'}],
            'children': [nxlog('value', source, topic),
                         {'module': 'dataset', 'config': {'name': 'depends_on', 'values': '.'}}]}


def structure(*children):
    return {'children': [{'name': 'entry', 'type': 'group',
                          'attributes': [{'name': 'NX_class', 'dtype': 'string', 'values': 'NXentry'}],
                          'children': list(children)}]}


def inserted(ns):
    """The names under /entry/parameters after insertion."""
    entry = ns['children'][0]
    params = next(c for c in entry['children'] if c.get('name') == 'parameters')
    return [c['name'] for c in params['children']]


class TestLoggedNames:
    def test_a_bare_nxlog_offers_its_name_and_its_source(self):
        assert logged_names(structure(nxlog('a3', 'a3'))) == {'a3'}

    def test_a_positioner_is_found_through_its_source(self):
        # The log itself is named 'value'; only the source still says 'a4'.
        assert logged_names(structure(nxpositioner('a4', 'a4'))) == {'a4'}

    def test_a_generic_log_name_is_not_a_parameter_name(self):
        assert not (logged_names(structure(nxpositioner('a4', 'a4'))) & GENERIC_LOG_NAMES)

    def test_a_real_positioner_pv_is_found_through_the_group_name(self):
        # Wired to a facility PV, the source resembles nothing in McStas -- the group
        # name is the only thing left tying the log to the parameter.
        assert logged_names(structure(nxlog('a3', 'BIFROST:MC:a3'))) == {'a3', 'BIFROST:MC:a3'}

    def test_a_log_built_only_from_links_is_not_coverage(self):
        # niess' linked_nxlog mirrors a log kept elsewhere -- and 'elsewhere' is the
        # /entry/parameters entry about to be added. Counting it would delete the
        # target its links resolve against.
        mirror = {'name': 'rotation_speed', 'type': 'group',
                  'attributes': [{'name': 'NX_class', 'dtype': 'string', 'values': 'NXlog'}],
                  'children': [
                      {'module': 'link', 'config': {'name': n,
                                                    'source': f'/entry/parameters/chopperspeed/{n}'}}
                      for n in ('value', 'time', 'description')]}
        assert logged_names(structure(mirror)) == set()

    def test_the_real_teaching_structure_is_all_mirrors(self):
        from json import load
        from pathlib import Path as _Path
        path = _Path(__file__).parent.parent.parent / 'niess' / 'teaching_nexus_structure.json'
        if not path.exists():
            pytest.skip('niess checkout not beside mccode-plumber')
        assert logged_names(load(path.open())) == set()

    def test_a_group_that_is_not_an_nxlog_offers_nothing(self):
        detector = {'name': 'a3', 'type': 'group',
                    'attributes': [{'name': 'NX_class', 'dtype': 'string', 'values': 'NXdetector'}],
                    'children': [{'module': 'ev44', 'config': {'source': 'arc=1', 'topic': 'det'}}]}
        assert logged_names(structure(detector)) == set()


class TestInsertion:
    def test_a_parameter_with_no_log_is_still_added(self):
        ns = add_pvs_to_nexus_structure(structure(), [pv('sample_rotation')])
        assert inserted(ns) == ['sample_rotation']

    def test_a_parameter_niess_already_logs_is_skipped(self):
        ns = add_pvs_to_nexus_structure(structure(nxlog('a3', 'a3')), [pv('a3')])
        assert inserted(ns) == []

    def test_a_parameter_behind_a_positioner_is_skipped(self):
        ns = add_pvs_to_nexus_structure(structure(nxpositioner('a4', 'a4')), [pv('a4')])
        assert inserted(ns) == []

    def test_matching_on_the_prefixed_source_also_skips(self):
        ns = add_pvs_to_nexus_structure(structure(nxlog('elsewhere', 'mcstas:a3')), [pv('a3')])
        assert inserted(ns) == []

    def test_only_the_covered_parameters_are_skipped(self):
        ns = add_pvs_to_nexus_structure(
            structure(nxlog('a3', 'a3'), nxpositioner('a4', 'a4')),
            [pv('a3'), pv('a4'), pv('chopper_speed'), pv('slit_width')],
        )
        assert inserted(ns) == ['chopper_speed', 'slit_width']

    def test_the_entries_added_do_not_mask_each_other(self):
        # Every inserted entry is itself an NXlog; collecting coverage inside the loop
        # would let the first insertion suppress a later duplicate silently.
        ns = add_pvs_to_nexus_structure(structure(), [pv('a3'), pv('chopper_speed')])
        assert inserted(ns) == ['a3', 'chopper_speed']

    def test_a_mirrored_parameter_is_still_added(self):
        # The teaching structure's links resolve only if the entry is created.
        mirror = {'name': 'rotation_speed', 'type': 'group',
                  'attributes': [{'name': 'NX_class', 'dtype': 'string', 'values': 'NXlog'}],
                  'children': [{'module': 'link',
                                'config': {'name': 'value',
                                           'source': '/entry/parameters/chopperspeed/value'}}]}
        ns = add_pvs_to_nexus_structure(structure(mirror), [pv('chopperspeed')])
        assert inserted(ns) == ['chopperspeed']

    def test_a_missing_key_is_still_an_error(self):
        broken = pv('a3')
        del broken['unit']
        with pytest.raises(RuntimeError, match='missing one or more required keys'):
            add_pvs_to_nexus_structure(structure(), [broken])
