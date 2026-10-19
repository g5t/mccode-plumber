"""The live top-dead-centre server.

Two properties carry the weight. The times are taken from a *grid instant* rather than
from a second reading of the clock, so a crossing and the pulse it is measured from share
one number; and nothing is published while no run is in progress.
"""
import multiprocessing
import unittest
from uuid import uuid4

import numpy as np

from mccode_plumber.conductor import NS, Chopper
from mccode_plumber.tdc import FakeTDC, main, parse_chopper

PERIOD = round(NS / 14.0)


class _Recorder:
    """Stands in for a SharedPV: keeps what was posted and when it was stamped."""

    def __init__(self):
        self.posts = []

    def post(self, value, timestamp=None):
        self.posts.append((value, timestamp))


def forwarded(value, stamp):
    """What the forwarder's `tdct` serialiser makes of a TDC PV: each element plus the
    PV's own stamp, in nanoseconds (forwarder `tdct_serialiser.py`)."""
    seconds, nanoseconds = stamp
    return np.asarray(value).astype(np.uint64) + (seconds * NS + nanoseconds)


def _wired(choppers, **kwargs):
    faker = FakeTDC(tuple(choppers), **kwargs)
    names = [c.tdc for c in choppers] + [faker.pulse_pv]
    faker.pvs = {name: _Recorder() for name in names}
    return faker


DISC = Chopper(name='psc1', tdc='psc1_tdc', speed='psc1speed', delay='psc1delay',
               park='psc1park')


class ParseChopperTest(unittest.TestCase):

    def test_the_four_required_names(self):
        self.assertEqual(parse_chopper('psc1,psc1_tdc,psc1speed,psc1delay'),
                         Chopper('psc1', 'psc1_tdc', 'psc1speed', 'psc1delay'))

    def test_park_is_optional(self):
        self.assertEqual(parse_chopper('psc1,a,b,c,d').park, 'd')

    def test_too_few_names_is_an_error(self):
        """Rather than a Chopper with a delay where its speed should be."""
        with self.assertRaises(ValueError):
            parse_chopper('psc1,psc1_tdc,psc1speed')


class PVDeclarationTest(unittest.TestCase):

    def test_every_name_a_chopper_needs_is_served(self):
        faker = FakeTDC((DISC,))
        names = {s.rsplit(':', 2)[0] for s in faker.strings()}
        self.assertEqual(names, {'psc1_tdc', 'psc1speed', 'psc1delay', 'psc1park',
                                 'pulse', 'tdc_run'})

    def test_the_tdc_channel_is_an_unsigned_64_bit_array(self):
        """Nanoseconds since the epoch needs 61 bits; a double quantises them to 256 ns."""
        self.assertIn('psc1_tdc:aL:[]', FakeTDC((DISC,)).strings())

    def test_the_tdc_channel_starts_empty(self):
        """Not `[0]`: a forwarder connecting before the run would record a crossing at
        the epoch, which is not a thing that happened."""
        strings = FakeTDC((DISC,)).strings()
        self.assertNotIn('psc1_tdc:aL:[0]', strings)

    def test_a_shared_parameter_is_declared_once(self):
        pair = (DISC, Chopper('psc2', 'psc2_tdc', 'psc1speed', 'psc2delay'))
        self.assertEqual(FakeTDC(pair).input_names().count('psc1speed'), 1)


class EmitTest(unittest.TestCase):
    """One pulse at a time, with the PVs replaced by recorders."""

    def setUp(self):
        self.faker = _wired([DISC])
        self.faker.values.update({'psc1speed': 14.0, 'psc1delay': 0.005})

    def test_the_pulse_sample_and_the_crossings_share_one_instant(self):
        """The load-bearing property. Stamping the reference from a fresh clock reading
        would put it *after* the crossings that are measured from it."""
        pulse_ns = 1_788_000_000_123_456_789
        self.faker.emit(pulse_ns)
        (_, pulse_stamp), = self.faker.pvs['pulse'].posts
        (_, tdc_stamp), = self.faker.pvs['psc1_tdc'].posts
        self.assertEqual(pulse_stamp, (1_788_000_000, 123_456_789))
        self.assertEqual(tdc_stamp, pulse_stamp)

    def test_the_pv_holds_offsets_from_its_stamp(self):
        """The forwarder adds the stamp to every element, so absolute times on the PV
        would reach the file with the epoch counted twice."""
        self.faker.emit(1_788_000_000_000_000_000)
        (offsets, _), = self.faker.pvs['psc1_tdc'].posts
        self.assertEqual(int(offsets[0]), 5_000_000)

    def test_what_is_forwarded_is_the_absolute_crossing_to_the_nanosecond(self):
        """Exactly: a float stamp would lose ~240 ns at today's epoch, and the forwarder
        adds back whatever the stamp says."""
        pulse_ns = 1_788_000_000_123_456_789
        self.faker.values.update({'psc1speed': 196.0})
        self.faker.emit(pulse_ns)
        (offsets, stamp), = self.faker.pvs['psc1_tdc'].posts
        expected = DISC.crossings(pulse_ns, self.faker.values)
        self.assertEqual([int(t) for t in forwarded(offsets, stamp)], expected)

    def test_the_vector_is_unsigned_64_bit(self):
        self.faker.emit(1_788_000_000_000_000_000)
        times, _ = self.faker.pvs['psc1_tdc'].posts[0]
        self.assertEqual(times.dtype, np.uint64)

    def test_a_parked_disc_posts_no_vector_but_still_a_pulse(self):
        """A stationary disc genuinely never trips its sensor. The pulse still happened."""
        self.faker.values['psc1speed'] = 0.0
        self.faker.emit(1_788_000_000_000_000_000)
        self.assertEqual(self.faker.pvs['psc1_tdc'].posts, [])
        self.assertEqual(len(self.faker.pvs['pulse'].posts), 1)


class GridTest(unittest.TestCase):
    """The clock and the sleep are substituted, so the grid can be inspected exactly."""

    def setUp(self):
        self.faker = _wired([DISC])
        self.faker.values.update({'psc1speed': 14.0, 'psc1delay': 0.0})
        self.now = 1_788_000_000_123_456_789
        self.slept = []

    def clock(self):
        return self.now

    def wait(self, seconds):
        self.slept.append(seconds)
        self.now += int(round(seconds * NS))

    def pulses(self):
        """The pulse each posted vector belongs to: its stamp, in nanoseconds."""
        return [seconds * NS + nanoseconds
                for _, (seconds, nanoseconds) in self.faker.pvs['psc1_tdc'].posts]

    def test_the_ticks_are_evenly_spaced_grid_instants(self):
        """Anchored on the epoch, not on start-up, and taken from the grid rather than
        from the moment of waking -- so replayed events land on the same ticks."""
        self.faker.running = True
        self.faker.run(ticks=3, clock=self.clock, wait=self.wait)
        pulses = self.pulses()
        self.assertEqual(len(pulses), 3)
        self.assertEqual([p % PERIOD for p in pulses], [0, 0, 0])
        self.assertEqual([b - a for a, b in zip(pulses, pulses[1:])], [PERIOD, PERIOD])

    def test_a_tick_never_lands_in_the_past(self):
        self.faker.running = True
        self.faker.run(ticks=3, clock=self.clock, wait=self.wait)
        self.assertTrue(all(s > 0 for s in self.slept))

    def test_nothing_is_published_while_no_run_is_in_progress(self):
        self.faker.run(ticks=3, clock=self.clock, wait=self.wait)
        self.assertEqual(self.faker.pvs['psc1_tdc'].posts, [])
        self.assertEqual(self.faker.pvs['pulse'].posts, [])

    def test_the_grid_keeps_its_phase_across_a_stop(self):
        """Only publishing is gated; skipping ticks while stopped would shift the phase
        and the crossings would stop agreeing with the events."""
        self.faker.running = True
        self.faker.run(ticks=1, clock=self.clock, wait=self.wait)
        self.faker.running = False
        self.faker.run(ticks=2, clock=self.clock, wait=self.wait)
        self.faker.running = True
        self.faker.run(ticks=1, clock=self.clock, wait=self.wait)
        first, last = self.pulses()
        self.assertEqual(last - first, 3 * PERIOD)


def serve(choppers, pulse_pv, run_pv):
    main(choppers, pulse_pv=pulse_pv, run_pv=run_pv)


class ServerTestCase(unittest.TestCase):
    """Against a real p4p server, as the rest of this package's tests do."""

    @classmethod
    def setUpClass(cls):
        import time
        from mccode_plumber.pva import context
        p = f't{uuid4().hex[:8]}:'
        cls.disc = Chopper(name='psc1', tdc=f'{p}psc1_tdc', speed=f'{p}psc1speed',
                           delay=f'{p}psc1delay', park=f'{p}psc1park')
        cls.pulse_pv, cls.run_pv = f'{p}pulse', f'{p}tdc_run'
        ctx = multiprocessing.get_context('spawn')
        cls.proc = ctx.Process(target=serve,
                               args=((cls.disc,), cls.pulse_pv, cls.run_pv), daemon=True)
        cls.proc.start()
        cls.ctx = context()
        deadline = time.time() + 30
        while time.time() < deadline:
            if not isinstance(cls.ctx.get(cls.disc.speed, throw=False), Exception):
                break
            time.sleep(0.2)
        else:
            cls.ctx.close()
            cls.proc.terminate()
            raise unittest.SkipTest('the top-dead-centre server did not start')

    @classmethod
    def tearDownClass(cls):
        cls.ctx.close()
        cls.proc.terminate()
        cls.proc.join(5)

    def setUp(self):
        self.ctx.put(self.run_pv, 0)

    def tearDown(self):
        self.ctx.put(self.run_pv, 0)

    def collect(self, seconds, speed=196.0, delay=0.0, run=1):
        """Watch the TDC channel while a run is (or is not) in progress."""
        import time
        from queue import Empty, Queue
        seen = Queue()
        sub = self.ctx.monitor(self.disc.tdc, seen.put)
        try:
            self.ctx.put(self.disc.speed, speed)
            self.ctx.put(self.disc.delay, delay)
            while True:                       # drop the initial value and anything stale
                try:
                    seen.get(timeout=0.3)
                except Empty:
                    break
            self.ctx.put(self.run_pv, run)
            time.sleep(seconds)
            self.ctx.put(self.run_pv, 0)
            out = []
            while True:
                try:
                    out.append(np.asarray(seen.get(timeout=0.2), dtype=np.uint64))
                except Empty:
                    return out
        finally:
            sub.close()

    def test_the_server_is_really_there(self):
        """A negative control: the rest of this class would pass against nothing at all
        if `get` quietly returned an error object."""
        self.assertTrue(self.proc.is_alive())
        self.assertIsInstance(self.ctx.get(f'{self.disc.tdc}-nope', throw=False), Exception)
        self.assertNotIsInstance(self.ctx.get(self.disc.tdc, throw=False), Exception)

    def test_vectors_arrive_on_the_pulse_grid(self):
        got = self.collect(0.5)
        self.assertGreaterEqual(len(got), 2)
        for times in got:
            self.assertEqual(times.dtype, np.uint64)
            self.assertEqual(len(times), 14)            # 196 Hz, fourteen turns a pulse
            self.assertEqual(int(times[0]) % PERIOD, 0)
            self.assertEqual(int(times[1]) - int(times[0]), round(NS / 196))

    def test_the_delay_offsets_every_crossing(self):
        got = self.collect(0.4, speed=14.0, delay=0.017)
        self.assertTrue(got)
        for times in got:
            self.assertEqual(int(times[0]) % PERIOD, 17_000_000 % PERIOD)

    def test_a_stopped_server_publishes_nothing(self):
        self.assertEqual(self.collect(0.5, run=0), [])

    def test_a_parked_disc_publishes_nothing(self):
        self.assertEqual(self.collect(0.5, speed=0.0), [])


if __name__ == '__main__':
    unittest.main()


#: A disc whose delay is the total of the ESS controller's three, as niess >= 0.12 writes
#: it: the delay PV is served, not set, and pulse_delay is one PV every disc shares.
TOTALLED = Chopper(name='psc1', tdc='psc1_tdc', speed='psc1_rotation_speed',
                   delay='psc1_delay', park='psc1_park_angle', delay_unit='ns',
                   experiment='psc1_experiment_delay', mechanical='psc1_mechanical_delay',
                   pulse='pulse_delay')
PSC2 = Chopper(name='psc2', tdc='psc2_tdc', speed='psc2_rotation_speed',
               delay='psc2_delay', delay_unit='ns', experiment='psc2_experiment_delay',
               mechanical='psc2_mechanical_delay', pulse='pulse_delay')


class TotalDelayTest(unittest.TestCase):

    def test_the_total_is_formed_as_the_controller_forms_it(self):
        """ESS evr_seq_calc: TotDly = MechDly/360/|Spd_S|*1e9 + BeamPosDly + ChopDly."""
        values = {'psc1_rotation_speed': -14.0, 'psc1_experiment_delay': 2e6,
                  'psc1_mechanical_delay': 90.0, 'pulse_delay': 1e4}
        total = 2e6 + 1e4 + 90.0 / 360 / 14 * 1e9
        self.assertAlmostEqual(TOTALLED.total_delay(values) * NS, total, places=3)
        self.assertEqual(TOTALLED.offsets(values)[0], round(total))

    def test_the_parts_are_what_a_run_sets_and_the_total_is_not(self):
        self.assertEqual([pv for pv, _ in TOTALLED.served()],
                         ['psc1_rotation_speed', 'psc1_experiment_delay',
                          'psc1_mechanical_delay', 'pulse_delay', 'psc1_park_angle'])

    def test_all_three_parts_or_none(self):
        with self.assertRaises(ValueError):
            Chopper(name='x', tdc='t', speed='s', delay='d', experiment='e')

    def test_the_parts_are_options_on_the_command_line(self):
        self.assertEqual(parse_chopper(
            'psc1,psc1_tdc,psc1_rotation_speed,psc1_delay,psc1_park_angle,delay_unit=ns,'
            'experiment=psc1_experiment_delay,mechanical=psc1_mechanical_delay,'
            'pulse=pulse_delay'), TOTALLED)


class ServedTotalTest(unittest.TestCase):

    def faker(self):
        faker = _wired((TOTALLED, PSC2))
        faker.pvs.update({name: _Recorder() for name in faker.total_names()})
        faker.values.update({'psc1_rotation_speed': 14.0, 'psc1_experiment_delay': 1e6,
                             'psc1_mechanical_delay': 0.0, 'pulse_delay': 1e4,
                             'psc2_rotation_speed': 14.0, 'psc2_experiment_delay': 2e6,
                             'psc2_mechanical_delay': 0.0})
        return faker

    def test_the_parts_are_inputs_and_a_shared_pulse_delay_is_one_pv(self):
        faker = FakeTDC((TOTALLED, PSC2))
        self.assertEqual(faker.input_names().count('pulse_delay'), 1)
        self.assertNotIn('psc1_delay', faker.input_names())
        self.assertIn('psc1_delay:d:0.0', faker.strings())

    def test_the_total_is_posted_on_the_pulse_and_again_only_when_it_changes(self):
        faker = self.faker()
        faker.emit(5 * PERIOD)
        faker.emit(6 * PERIOD)
        posts = faker.pvs['psc1_delay'].posts
        self.assertEqual(len(posts), 1)
        self.assertAlmostEqual(posts[0][0], 1e6 + 1e4)
        self.assertEqual(posts[0][1], divmod(5 * PERIOD, NS))
        faker.values['pulse_delay'] = 2e4          # moves every disc
        faker.emit(7 * PERIOD)
        self.assertAlmostEqual(faker.pvs['psc1_delay'].posts[-1][0], 1e6 + 2e4)
        self.assertAlmostEqual(faker.pvs['psc2_delay'].posts[-1][0], 2e6 + 2e4)

    def test_the_crossings_follow_the_total(self):
        faker = self.faker()
        faker.emit(5 * PERIOD)
        offsets, _ = faker.pvs['psc1_tdc'].posts[-1]
        self.assertEqual(list(offsets), [round(1e6 + 1e4)])
