"""Reading the choppers out of the NeXus structure a run is going to write.

The structure is the source of truth on purpose: it already says which channel a disc's
crossings arrive on and which parameters they follow from, so taking the names from it is
what guarantees the PVs `mp-tdc` serves are the sources the file-writer is waiting on.
"""
import unittest

from mccode_plumber.manage.orchestrate import (
    chopper_forwarder_streams, get_chopper_specs, get_pulse_stream,
)


def log(name, module, source, topic='choppers'):
    return {'name': name, 'type': 'group',
            'children': [{'module': module, 'config': {'source': source, 'topic': topic}}],
            'attributes': [{'name': 'NX_class', 'values': 'NXlog'}]}


def disc(name='psc1', logs=None):
    return {'name': name, 'type': 'group',
            'children': logs if logs is not None else [
                log('rotation_speed', 'f144', f'{name}speed'),
                log('top_dead_center', 'tdct', f'{name}_tdc'),
                log('mark_delay', 'f144', f'{name}delay'),
                log('park_angle', 'f144', f'{name}park'),
            ],
            'attributes': [{'name': 'NX_class', 'values': 'NXdisk_chopper'}]}


def nested(*groups):
    """Choppers are buried several levels down in a real structure."""
    return {'children': [{'name': 'entry', 'children': [
        {'name': 'instrument', 'children': list(groups)}]}]}


class SimulatedChopperTest(unittest.TestCase):

    def test_the_four_names_a_crossing_needs(self):
        (chopper, topic), = get_chopper_specs(nested(disc()))
        self.assertEqual(chopper.name, 'psc1')
        self.assertEqual(chopper.tdc, 'psc1_tdc')
        self.assertEqual(chopper.speed, 'psc1speed')
        self.assertEqual(chopper.delay, 'psc1delay')
        self.assertEqual(chopper.park, 'psc1park')
        self.assertEqual(topic, 'choppers')

    def test_every_disc_in_the_structure_is_found(self):
        specs = get_chopper_specs(nested(disc('psc1'), disc('psc2')))
        self.assertEqual([c.name for c, _ in specs], ['psc1', 'psc2'])

    def test_a_disc_that_cannot_park_is_still_a_disc(self):
        logs = [log('rotation_speed', 'f144', 'psc1speed'),
                log('top_dead_center', 'tdct', 'psc1_tdc'),
                log('mark_delay', 'f144', 'psc1delay')]
        (chopper, _), = get_chopper_specs(nested(disc(logs=logs)))
        self.assertIsNone(chopper.park)

    def test_a_group_with_no_tdc_log_is_not_ours_to_fake(self):
        logs = [log('rotation_speed', 'f144', 'psc1speed'),
                log('mark_delay', 'f144', 'psc1delay')]
        self.assertEqual(get_chopper_specs(nested(disc(logs=logs))), [])

    def test_a_real_chopper_is_left_to_its_control_system(self):
        """ESS `delay` is the controller's electronic delay, off a real PV, and the
        crossings are measured by an actual pickup. Neither is a thing to fake."""
        root = 'BIFRO-ChpSy1:Chop-PSC-101'
        logs = [log('rotation_speed', 'f144', f'{root}:Spd_R'),
                log('top_dead_center', 'tdct', f'{root}:00-TS-I'),
                log('delay', 'f144', f'{root}:TotDly')]
        self.assertEqual(get_chopper_specs(nested(disc(logs=logs))), [])

    def test_a_disc_with_no_delay_at_all_is_refused(self):
        """A legacy instrument's knob is a phase in degrees, not a delay in seconds.
        Reading one as the other is wrong by a factor of several hundred, in a file that
        looks fine -- so say so instead."""
        logs = [log('rotation_speed', 'f144', 'psc1speed'),
                log('top_dead_center', 'tdct', 'psc1_tdc')]
        with self.assertRaises(ValueError) as caught:
            get_chopper_specs(nested(disc(logs=logs)))
        self.assertIn('psc1', str(caught.exception))


class PulseStreamTest(unittest.TestCase):

    def source(self, name='pulse'):
        group = {'name': 'neutron_prod_info', 'type': 'group',
                 'children': [log('current_log', 'f144', name, topic='params')],
                 'attributes': [{'name': 'NX_class', 'values': 'NXsource'}]}
        return nested(group)

    def test_the_reference_sample_is_found(self):
        self.assertEqual(get_pulse_stream(self.source()), ('pulse', 'params'))

    def test_a_structure_without_one_says_so(self):
        self.assertIsNone(get_pulse_stream(nested(disc())))


class ForwarderStreamTest(unittest.TestCase):

    def setUp(self):
        self.specs = get_chopper_specs(nested(disc()))
        self.streams = chopper_forwarder_streams(self.specs, ('pulse', 'params'))
        self.by_source = {s['source']: s for s in self.streams}

    def test_the_crossings_are_tdct_and_nothing_else_is(self):
        """A vector of absolute times, not a value: `f144` cannot carry it."""
        self.assertEqual(self.by_source['psc1_tdc']['module'], 'tdct')
        self.assertEqual({s['module'] for s in self.streams} - {'tdct'}, {'f144'})

    def test_every_served_pv_is_declared(self):
        self.assertEqual(set(self.by_source),
                         {'psc1_tdc', 'psc1speed', 'psc1delay', 'psc1park', 'pulse'})

    def test_the_names_are_not_prefixed(self):
        """They came out of the structure already; prefixing them would stop the sources
        matching the ones the writer is waiting on."""
        self.assertTrue(all(not s['source'].startswith('mcstas:') for s in self.streams))

    def test_the_topics_come_from_the_structure(self):
        self.assertEqual(self.by_source['psc1_tdc']['topic'], 'choppers')
        self.assertEqual(self.by_source['pulse']['topic'], 'params')


try:
    from niess.components import Section
    from niess.components.chopper import DiscChopper
    from niess.instrument import Instrument, Mount
    from niess.nexus import to_nexus_structure
    from scipp import scalar, vector
    from scipp.spatial import rotations_from_rotvecs
    HAVE_NIESS = True
except ImportError:
    HAVE_NIESS = False


@unittest.skipUnless(HAVE_NIESS, 'niess is not installed')
class NiessStructureTest(unittest.TestCase):
    """Against what niess actually emits, rather than a hand-built shape."""

    def structure(self):
        cal = {'name': 'psc1', 'position': vector([0, 0, 4.4], unit='m'),
               'orientation': rotations_from_rotvecs(vector([0, 0, 0.0], unit='deg')),
               'radius': scalar(0.35, unit='m'), 'height': scalar(0.06, unit='m'),
               'angle': scalar(170., unit='deg'), 'frequency': scalar(14., unit='Hz'),
               'delay': scalar(0., unit='s'), 'beam_angle': scalar(180., unit='deg')}

        class Chopped(Section):
            disc: DiscChopper
            _flat: bool = True

        instrument = Instrument(
            name='chopped',
            parts=(Mount(name='m', content=Chopped(disc=DiscChopper.from_calibration(cal))),))
        return to_nexus_structure(instrument)

    def test_the_emitted_names_are_the_parameter_names(self):
        (chopper, _), = get_chopper_specs(self.structure())
        self.assertEqual((chopper.tdc, chopper.speed, chopper.delay, chopper.park),
                         ('psc1_tdc', 'psc1speed', 'psc1delay', 'psc1park'))

    def test_the_pulse_reference_is_found(self):
        self.assertIsNotNone(get_pulse_stream(self.structure()))


if __name__ == '__main__':
    unittest.main()
