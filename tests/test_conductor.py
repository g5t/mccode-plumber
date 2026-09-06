"""The bridge from a replayed simulation into EPICS, choppers included.

The load-bearing property is that every timestamp comes from the pulse instant the
replayer reports. A value stamped from a clock of its own would land before the pulse it
belongs to, because the replayer sleeps to the next grid tick *after* publishing the
point's parameters -- at 14 Hz that is up to 71 ms, a whole chopper revolution.
"""
import multiprocessing
import unittest
from uuid import uuid4

import numpy as np

from mccode_plumber.conductor import NS, Chopper, ChopperPublisher, chopper_pv_strings
from mccode_plumber.epics import convert_strings_to_nt, main

PULSE = 1_788_000_000_000_000_000
ROOT = 'BIFRO-ChpSy1:Chop-PSC-101'


def serve(strings, prefix):
    main(convert_strings_to_nt(strings), prefix=prefix, filename_required=False)


class ChopperArithmeticTestCase(unittest.TestCase):
    """No server needed: the times follow from the parameters and the pulse."""

    def setUp(self):
        self.disc = Chopper(name='psc1', tdc=f'{ROOT}:00-TS-I',
                            speed='psc1speed', delay='psc1delay')

    def test_a_disc_at_the_source_frequency_crosses_once_a_pulse(self):
        times = self.disc.crossings(PULSE, {'psc1speed': 14.0, 'psc1delay': 0.0})
        self.assertEqual(times, [PULSE])

    def test_a_faster_disc_crosses_once_per_turn(self):
        times = self.disc.crossings(PULSE, {'psc1speed': 196.0, 'psc1delay': 0.0})
        self.assertEqual(len(times), 14)                      # 196 / 14
        self.assertEqual(times[0], PULSE)
        self.assertEqual(times[1] - times[0], round(NS / 196))

    def test_the_delay_offsets_every_crossing(self):
        times = self.disc.crossings(PULSE, {'psc1speed': 14.0, 'psc1delay': 0.017})
        self.assertEqual(times, [PULSE + 17_000_000])

    def test_values_arrive_as_strings(self):
        """`ParameterPublisher.publish` hands over strings, not numbers."""
        self.assertEqual(self.disc.crossings(PULSE, {'psc1speed': '14.0', 'psc1delay': '0.0'}),
                         [PULSE])

    def test_a_parked_disc_never_triggers_its_sensor(self):
        """Not an error and not an empty measurement -- a stationary disc has no mark
        crossings to report, so it reports none."""
        self.assertEqual(self.disc.crossings(PULSE, {'psc1speed': 0.0, 'psc1delay': 0.0}), [])

    def test_crossings_are_not_clipped_to_their_pulse(self):
        """With a large delay and a fast disc the later crossings legitimately fall
        after the next pulse begins. Truncating them would drop real events."""
        times = self.disc.crossings(PULSE, {'psc1speed': 196.0, 'psc1delay': 0.060})
        next_pulse = PULSE + round(NS / 14)
        self.assertTrue(any(t > next_pulse for t in times))
        self.assertTrue(all(t >= PULSE for t in times))


class MailboxTestCase(unittest.TestCase):
    """Against a real p4p server, as the rest of this package's tests do."""

    @classmethod
    def setUpClass(cls):
        cls.prefix = f"t{uuid4().hex[:8]}:"
        cls.disc = Chopper(name='psc1', tdc=f'{ROOT}:00-TS-I',
                           speed='psc1speed', delay='psc1delay')
        strings = chopper_pv_strings([cls.disc]) + ['psc1speed:d:0.0', 'psc1delay:d:0.0']
        ctx = multiprocessing.get_context('spawn')
        cls.proc = ctx.Process(target=serve, args=(strings, cls.prefix), daemon=True)
        cls.proc.start()

        import time
        from p4p.client.thread import Context
        cls.ctx = Context('pva')
        deadline = time.time() + 30
        ready = False
        while time.time() < deadline:
            if not isinstance(cls.ctx.get(cls.prefix + 'psc1speed', throw=False), Exception):
                ready = True
                break
            time.sleep(0.2)
        if not ready:
            cls.ctx.close()
            cls.proc.terminate()
            raise unittest.SkipTest('the mailbox server did not start')

    @classmethod
    def tearDownClass(cls):
        cls.ctx.close()
        cls.proc.terminate()
        cls.proc.join(5)

    def publisher(self):
        return ChopperPublisher(
            prefix=self.prefix,
            choppers=(Chopper(name='psc1', tdc=self.prefix + self.disc.tdc,
                              speed='psc1speed', delay='psc1delay'),),
            pulse_pv=self.prefix + 'pulse',
            context=self.ctx)

    def test_a_timestamp_vector_survives_the_round_trip_exactly(self):
        """61 bits of nanoseconds. A double would quantise them to 256 ns."""
        pub = self.publisher()
        pub.publish(0, 'psc1speed', '196.0', 'Hz')
        pub.publish(0, 'psc1delay', '0.0', 's')
        pub.pulse_ready(0, PULSE)

        got = np.asarray(self.ctx.get(self.prefix + self.disc.tdc), dtype=np.uint64)
        expected = np.asarray(pub.choppers[0].crossings(
            PULSE, {'psc1speed': 196.0, 'psc1delay': 0.0}), dtype=np.uint64)
        np.testing.assert_array_equal(got, expected)
        self.assertEqual(int(got[0]), PULSE)

    def test_the_server_is_really_there(self):
        """A negative control: everything else here would pass against nothing at all
        if `get` quietly returned an error object, so prove an absent PV times out
        while a declared one does not."""
        self.assertTrue(self.proc.is_alive())
        missing = self.ctx.get(self.prefix + 'no-such-pv', throw=False)
        self.assertIsInstance(missing, Exception)
        self.assertNotIsInstance(self.ctx.get(self.prefix + 'psc1speed', throw=False), Exception)

    def test_a_parameter_reaches_its_pv(self):
        pub = self.publisher()
        pub.publish(0, 'psc1speed', '14.0', 'Hz')
        self.assertEqual(self.ctx.get(self.prefix + 'psc1speed'), 14.0)

    def test_a_parked_disc_writes_no_timestamps(self):
        pub = self.publisher()
        pub.publish(0, 'psc1speed', '0.0', 'Hz')
        pub.publish(0, 'psc1delay', '0.0', 's')
        before = np.asarray(self.ctx.get(self.prefix + self.disc.tdc), dtype=np.uint64)
        pub.pulse_ready(1, PULSE + 10 ** 9)
        after = np.asarray(self.ctx.get(self.prefix + self.disc.tdc), dtype=np.uint64)
        np.testing.assert_array_equal(before, after)

    def test_every_timestamp_comes_from_the_reported_pulse(self):
        """The requirement: one reference, shared by everything."""
        pub = self.publisher()
        pub.publish(0, 'psc1speed', '14.0', 'Hz')
        pub.publish(0, 'psc1delay', '0.005', 's')
        for point, pulse in enumerate((PULSE, PULSE + round(NS / 14))):
            pub.pulse_ready(point, pulse)
            got = np.asarray(self.ctx.get(self.prefix + self.disc.tdc), dtype=np.uint64)
            self.assertEqual(int(got[0]), pulse + 5_000_000)


if __name__ == '__main__':
    unittest.main()
