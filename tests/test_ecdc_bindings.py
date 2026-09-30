"""A simulated BIFROST named the way ECDC binds the real instrument.

niess can write a simulation whose NeXus structure uses the real instrument's topics
and PV names, each prefixed `mcstas:` so it can never be taken for the real PV. Each
log it fills from a parameter says which one, as a `simulation_parameter` attribute.
The sources then look nothing like the parameters, so everything plumber serves,
forwards and sets is read from that attribute rather than inferred from a name.

The fixtures are cut from what niess actually writes; see
`data/make_bifrost_structure_excerpts.py`.
"""
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from mccode_plumber.conductor import Chopper
from mccode_plumber.manage.orchestrate import (
    SimulatedLog, chopper_forwarder_streams, get_chopper_specs, get_pulse_stream,
    get_simulated_logs, get_stream_modules, simulated_log_forwarder_streams,
    simulated_log_strings, topics_of,
)

EXCERPTS = json.loads((Path(__file__).parent / 'data' / 'bifrost_structure_excerpts.json')
                      .read_text())
ECDC, SIMULATED = EXCERPTS['ecdc'], EXCERPTS['simulated']
PSC1 = 'BIFRO-ChpSy1:Chop-PSC-101'


class _FakeExpr:
    def __init__(self, value):
        self.value = value
        self.has_value = True


class _FakeParameter:
    def __init__(self, name, value):
        self.name = name
        self.value = _FakeExpr(value)


class _FakeInstr:
    def __init__(self, **values):
        self.parameters = tuple(_FakeParameter(k, v) for k, v in values.items())


class _FakeContext:
    def __init__(self):
        self.puts = []

    def put(self, name, value):
        self.puts.append((name, value))


# -- choppers ---------------------------------------------------------------------

class BoundChopperTest(unittest.TestCase):

    def setUp(self):
        self.specs = {c.name: (c, topic) for c, topic in get_chopper_specs(ECDC)}

    def test_a_bound_disc_is_simulated_because_it_names_its_parameters(self):
        """Its logs are called what a real disc's are; only the attribute tells them apart."""
        self.assertEqual(sorted(self.specs), ['bandwidth_chopper_2', 'pulse_shaping_chopper_1'])

    def test_the_pvs_are_the_prefixed_real_ones(self):
        disc, topic = self.specs['pulse_shaping_chopper_1']
        self.assertEqual(disc.tdc, f'mcstas:{PSC1}:00-TS-I')
        self.assertEqual(disc.speed, f'mcstas:{PSC1}:Spd_R')
        self.assertEqual(disc.delay, f'mcstas:{PSC1}:TotDly')
        self.assertEqual(disc.park, f'mcstas:{PSC1}:Pos_R')
        self.assertEqual(topic, 'bifrost_choppers')

    def test_the_values_come_from_the_parameters_the_logs_name(self):
        disc, _ = self.specs['pulse_shaping_chopper_1']
        self.assertEqual(disc.parameter('speed'), 'pulse_shaping_chopper_1_rotation_speed')
        self.assertEqual(disc.parameter('delay'), 'pulse_shaping_chopper_1_delay')
        self.assertEqual(disc.parameter('park'), 'pulse_shaping_chopper_1_park_angle')

    def test_the_delay_is_in_nanoseconds_because_its_log_says_so(self):
        disc, _ = self.specs['pulse_shaping_chopper_1']
        self.assertEqual(disc.delay_unit, 'ns')

    def test_a_plain_simulated_disc_is_found_the_same_way(self):
        (disc, topic), *_ = get_chopper_specs(SIMULATED)
        self.assertEqual(disc.speed, 'pulse_shaping_chopper_1_rotation_speed')
        self.assertEqual(disc.parameter('speed'), disc.speed)
        self.assertEqual(disc.delay_unit, 'ns')

    def test_a_real_disc_is_still_left_alone(self):
        """Strip the attributes and the same group is a real controller's."""
        real = json.loads(json.dumps(ECDC).replace('"simulation_parameter"', '"other"'))
        self.assertEqual(get_chopper_specs(real), [])

    def test_the_chopper_values_are_forwarded_on_the_chopper_topic(self):
        streams = chopper_forwarder_streams(list(self.specs.values()), get_pulse_stream(ECDC))
        by_source = {s['source']: s for s in streams}
        self.assertEqual(by_source[f'mcstas:{PSC1}:00-TS-I']['module'], 'tdct')
        self.assertEqual(by_source[f'mcstas:{PSC1}:TotDly']['topic'], 'bifrost_choppers')


class DelayUnitTest(unittest.TestCase):
    """The crossings depend on the delay as a time, whatever unit it is published in."""

    def test_nanoseconds_and_seconds_give_the_same_crossings(self):
        seconds = Chopper('d', 'tdc', 'speed', 'delay')
        nanoseconds = Chopper('d', 'tdc', 'speed', 'delay', delay_unit='ns')
        pulse = 1_700_000_000_000_000_000
        self.assertEqual(seconds.crossings(pulse, {'speed': 28.0, 'delay': 0.0123}),
                         nanoseconds.crossings(pulse, {'speed': 28.0, 'delay': 12_300_000}))

    def test_values_may_be_keyed_by_parameter_for_the_replayer(self):
        disc = Chopper('d', 'tdc', 'pv:speed', 'pv:delay', speed_parameter='s',
                       delay_parameter='t', delay_unit='ns')
        self.assertEqual(disc.crossings(0, {'s': 14.0, 't': 1000}, by_parameter=True)[0], 1000)
        self.assertEqual(disc.crossings(0, {'pv:speed': 14.0, 'pv:delay': 1000})[0], 1000)

    def test_a_unit_that_is_not_a_time_is_refused(self):
        with self.assertRaises(ValueError):
            Chopper('d', 'tdc', 'speed', 'delay', delay_unit='degrees')

    def test_the_unit_reaches_mp_tdc(self):
        from types import SimpleNamespace
        from mccode_plumber.tdc import parse_chopper
        from mccode_plumber.manage.tdc import TDCFaker
        disc = Chopper('d', 'a:tdc', 'a:speed', 'a:delay', 'a:park', delay_unit='ns')
        # just the fields the command is built from; a whole Manager wants a process
        faker = SimpleNamespace(_command=Path('mp-tdc'), pulse_pv='pulse', run_pv='tdc_run',
                                rate=14.0, choppers=(disc,))
        argv = TDCFaker.__run_command__(faker)
        text = argv[argv.index('--chopper') + 1]
        self.assertEqual(text, 'd,a:tdc,a:speed,a:delay,a:park,delay_unit=ns')
        self.assertEqual(parse_chopper(text), disc)

    def test_seconds_stay_implicit_on_the_command_line(self):
        from mccode_plumber.tdc import parse_chopper
        self.assertEqual(parse_chopper('d,tdc,speed,delay').delay_unit, 's')
        with self.assertRaises(ValueError):
            parse_chopper('d,tdc,speed,delay,colour=red')


# -- the pulse reference ------------------------------------------------------------

class PulseTest(unittest.TestCase):

    def test_the_pulse_is_the_accelerators_current(self):
        self.assertEqual(get_pulse_stream(ECDC),
                         ('mcstas:TD-M:Ctrl-EVR-1:DbufBCurr-I', 'tn_data_general'))

    def test_the_older_name_is_still_read(self):
        old = {'children': [{'name': 'neutron_prod_info', 'type': 'group',
                             'attributes': [{'name': 'NX_class', 'values': 'NXsource'}],
                             'children': [{'name': 'current_log', 'type': 'group',
                                           'children': [{'module': 'f144', 'config': {
                                               'source': 'pulse', 'topic': 'choppers'}}]}]}]}
        self.assertEqual(get_pulse_stream(old), ('pulse', 'choppers'))


# -- everything else a parameter fills --------------------------------------------------

class SimulatedLogTest(unittest.TestCase):

    def setUp(self):
        choppers = [c for c, _ in get_chopper_specs(ECDC)]
        self.logs = {log.parameter: log for log in get_simulated_logs(ECDC, choppers)}

    def test_every_driven_axis_is_found_and_no_chopper_value(self):
        self.assertEqual(sorted(self.logs), sorted(
            ['detector_tank_angle', 'sample_rotation']
            + [f'divergence_slit_1_{e}' for e in ('left', 'right')]
            + [f'{g}_{e}' for g in ('mask', 'sample_jaws')
               for e in ('left', 'right', 'bottom', 'top')]))

    def test_a_bound_axis_names_its_prefixed_pv_and_topic(self):
        log = self.logs['sample_rotation']
        self.assertEqual(log.source, 'mcstas:BIFRO-SpRot:MC-RotZ-01:Mtr.RBV')
        self.assertEqual(log.topic, 'bifrost_motion')
        self.assertEqual(log.dtype, 'double')

    def test_the_mailbox_serves_each_one_from_its_parameters_default(self):
        parameters = _FakeInstr(sample_rotation=12.5, sample_jaws_left=-35.0).parameters
        strings = simulated_log_strings(
            [self.logs['sample_rotation'], self.logs['sample_jaws_left']], parameters)
        self.assertEqual(strings, [
            'mcstas:BIFRO-SpRot:MC-RotZ-01:Mtr.RBV:d:12.5',
            'mcstas:BIFRO-SpSl1:MC-SlYp-01:PzMtr.RBV:d:-35.0',
        ])

    def test_a_log_already_served_as_a_parameter_is_not_served_twice(self):
        """The mask is not bound, so its source is `mcstas:<parameter>` -- the mailbox's
        own name for that parameter -- and a second server of it would collide."""
        self.assertEqual(self.logs['mask_left'].source, 'mcstas:mask_left')
        self.assertEqual(simulated_log_strings([self.logs['mask_left']], ()), [])

    def test_each_is_forwarded_on_its_own_topic(self):
        streams = simulated_log_forwarder_streams([self.logs['sample_rotation']])
        self.assertEqual(streams, [dict(source='mcstas:BIFRO-SpRot:MC-RotZ-01:Mtr.RBV',
                                        module='f144', topic='bifrost_motion')])

    def test_a_plain_simulated_file_is_served_under_the_parameter_names(self):
        """Its sources are the bare parameter names, which the mailbox did not serve --
        why a simulated run's positioner logs used to be written empty."""
        logs = {log.parameter: log for log in get_simulated_logs(SIMULATED)}
        self.assertEqual(logs['sample_jaws_top'].source, 'sample_jaws_top')
        self.assertEqual(simulated_log_strings([logs['sample_jaws_top']],
                                               _FakeInstr(sample_jaws_top=35.0).parameters),
                         ['sample_jaws_top:d:35.0'])

    def test_every_topic_the_file_uses_is_registered(self):
        topics = topics_of(get_stream_modules(ECDC))
        for topic in ('bifrost_motion', 'bifrost_choppers', 'tn_data_general',
                      'bifrost_detector'):
            self.assertIn(topic, topics)


class PerPointTest(unittest.TestCase):
    """Before each point, every served PV is put the value its parameter has there."""

    def run_point(self, pars, choppers=(), logs=(), **defaults):
        from mccode_plumber.splitrun import chopper_parameters_callback_with_arguments
        context = _FakeContext()
        with patch('p4p.client.thread.Context', lambda *a, **k: context):
            callback, _ = chopper_parameters_callback_with_arguments(
                _FakeInstr(**defaults), list(choppers), 'tdc_run', logs=list(logs))
            callback(pars=pars)
        return dict(context.puts)

    def test_a_scanned_rotation_reaches_its_bound_pv(self):
        log = SimulatedLog('sample_rotation', 'mcstas:BIFRO-SpRot:MC-RotZ-01:Mtr.RBV',
                           'bifrost_motion', 'double')
        puts = self.run_point({'sample_rotation': 30.0}, logs=[log], sample_rotation=0.0)
        self.assertEqual(puts, {'mcstas:BIFRO-SpRot:MC-RotZ-01:Mtr.RBV': 30.0})

    def test_a_bound_disc_is_set_from_its_parameters(self):
        (disc, _), *_ = get_chopper_specs(ECDC)
        puts = self.run_point({'pulse_shaping_chopper_1_delay': 5e6}, choppers=[disc],
                              pulse_shaping_chopper_1_rotation_speed=196.0,
                              pulse_shaping_chopper_1_delay=0.0,
                              pulse_shaping_chopper_1_park_angle=0.0)
        self.assertEqual(puts[f'mcstas:{PSC1}:Spd_R'], 196.0)
        self.assertEqual(puts[f'mcstas:{PSC1}:TotDly'], 5e6)
        self.assertEqual(puts['tdc_run'], 1)

    def test_no_run_pv_without_a_chopper(self):
        log = SimulatedLog('a', 'mcstas:A', 'motion', 'double')
        self.assertNotIn('tdc_run', self.run_point({}, logs=[log], a=1.0))


class LoggedNamesTest(unittest.TestCase):

    def test_a_bound_log_counts_as_its_parameter(self):
        """So /entry/parameters does not get a second log of the same quantity."""
        from mccode_plumber.writer import logged_names
        names = logged_names(ECDC)
        for parameter in ('sample_rotation', 'detector_tank_angle', 'sample_jaws_left',
                          'pulse_shaping_chopper_1_delay'):
            self.assertIn(parameter, names)


class EveryParameterTest(unittest.TestCase):
    """With a prefix, every numeric parameter is put to its mailbox PV before the point.

    That is what fills /entry/parameters, and what the instrument's UpdateEPICS
    component used to do from inside the simulation.
    """

    def run_point(self, pars, *declarations, logs=()):
        from mccode_antlr.common import InstrumentParameter
        from mccode_plumber.splitrun import parameter_pvs_callback_with_arguments
        instr = _FakeInstr()
        instr.parameters = tuple(InstrumentParameter.parse(d) for d in declarations)
        context = _FakeContext()
        with patch('p4p.client.thread.Context', lambda *a, **k: context):
            callback, _ = parameter_pvs_callback_with_arguments(
                instr, [], 'tdc_run', logs=list(logs), prefix='mcstas:')
            callback(pars=pars)
        return context.puts

    def test_every_numeric_parameter_reaches_its_mailbox_pv(self):
        puts = dict(self.run_point({'a3': 30.0}, 'double a3 = 0', 'int order = 14',
                                   'string mcpl_filename = "x"'))
        self.assertEqual(puts, {'mcstas:a3': 30.0, 'mcstas:order': 14})

    def test_an_integer_parameter_is_put_as_an_integer(self):
        puts = dict(self.run_point({'order': 13}, 'int order = 14'))
        self.assertIsInstance(puts['mcstas:order'], int)

    def test_a_pv_that_is_both_a_log_and_a_parameter_is_put_once(self):
        log = SimulatedLog('mask_left', 'mcstas:mask_left', 'bifrost_motion', 'double')
        puts = self.run_point({}, 'double mask_left = -25', logs=[log])
        self.assertEqual(puts, [('mcstas:mask_left', -25.0)])

    def test_the_old_name_still_works(self):
        from mccode_plumber import splitrun
        self.assertIs(splitrun.chopper_parameters_callback_with_arguments,
                      splitrun.parameter_pvs_callback_with_arguments)
