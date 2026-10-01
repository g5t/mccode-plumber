"""Stopping a file-writer job, and making sure one is stopped whatever happens.

The file-writer answers a stop within milliseconds, but closing a large file can then
keep it silent for longer than the 15 s after which the status tracker calls a job
`TIMEOUT`. And a simulation that fails must not leave its job running: the writer stays
busy, and the next run's job times out waiting to start.
"""
import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from mccode_plumber.file_writer_control.JobStatus import JobState
from mccode_plumber.manage import orchestrate
from mccode_plumber.manage.orchestrate import stop_writer, wait_for_job_end


class FakePool:
    """Reports the states it is given, one per call, repeating the last."""

    def __init__(self, *states):
        self.states = list(states)
        self.stops = []

    def get_job_state(self, job_id):
        return self.states.pop(0) if len(self.states) > 1 else self.states[0]

    def get_job_status(self, job_id):
        return SimpleNamespace(message='disk full')

    def try_send_stop_now(self, service_id, job_id):
        self.stops.append(job_id)


class Clock:
    """Advances one second each time the wait sleeps."""

    def __init__(self):
        self.now = datetime(2026, 10, 1)

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += timedelta(seconds=seconds)


class WaitTest(unittest.TestCase):

    def wait(self, pool, timeout=60):
        clock = Clock()
        return wait_for_job_end(pool, 'job', timeout, clock=clock, sleep=clock.sleep)

    def test_a_silent_writer_is_waited_through(self):
        """TIMEOUT is the tracker hearing nothing for 15 s, not the writer answering."""
        pool = FakePool(JobState.WRITING, JobState.TIMEOUT, JobState.TIMEOUT, JobState.DONE)
        self.assertEqual(self.wait(pool), JobState.DONE)

    def test_an_error_ends_the_wait(self):
        self.assertEqual(self.wait(FakePool(JobState.WRITING, JobState.ERROR)), JobState.ERROR)

    def test_the_wait_ends_at_the_deadline(self):
        self.assertEqual(self.wait(FakePool(JobState.TIMEOUT), timeout=5), JobState.TIMEOUT)


class StopTest(unittest.TestCase):

    def test_the_pool_that_started_the_job_stops_it(self):
        """A new pool only starts listening now, and can miss the writer's answer."""
        pool = FakePool(JobState.DONE)
        with patch.object(orchestrate, '_writer_pool', side_effect=AssertionError('new pool')):
            self.assertEqual(stop_writer('broker', 'job', pool=pool), JobState.DONE)
        self.assertEqual(pool.stops, ['job'])

    def test_giving_up_says_how_to_kill_the_job(self):
        pool = FakePool(JobState.TIMEOUT)
        with patch.object(orchestrate, 'wait_for_job_end', return_value=JobState.TIMEOUT), \
                patch('builtins.print') as printed:
            stop_writer('broker', 'job-1', timeout=5, pool=pool)
        text = ' '.join(str(a) for call in printed.call_args_list for a in call.args)
        self.assertIn('mp-writer-kill job-1', text)
        self.assertIn('TIMEOUT', text)

    def test_an_error_is_reported_with_the_writers_message(self):
        pool = FakePool(JobState.ERROR)
        with patch('builtins.print') as printed:
            stop_writer('broker', 'job', timeout=5, pool=pool)
        self.assertIn('disk full', ' '.join(str(a) for c in printed.call_args_list
                                            for a in c.args))


class OrchestrateTest(unittest.TestCase):
    """Whatever the simulation does, its writer job is stopped and the forwarder reset."""

    def run_orchestrate(self, simulate, pool=FakePool(JobState.DONE)):
        calls = []
        instr = SimpleNamespace(name='inst', parameters=())
        with patch.object(orchestrate, 'start_writer', return_value=('job', pool)), \
                patch.object(orchestrate, 'stop_writer',
                             side_effect=lambda *a, **k: calls.append(('stop', k.get('pool')))), \
                patch.object(orchestrate, 'stop_faking_tdc',
                             side_effect=lambda c: calls.append('tdc')), \
                patch.object(orchestrate, 'augment_structure', side_effect=lambda p, s, t: s), \
                patch('mccode_plumber.forwarder.configure_forwarder'), \
                patch('mccode_plumber.forwarder.reset_forwarder',
                      side_effect=lambda *a, **k: calls.append('reset')), \
                patch('restage.splitrun.splitrun_args', side_effect=simulate):
            try:
                orchestrate.orchestrate(instr, {}, 'broker', {'args': None},
                                        nexus_file='/tmp/orchestrate-test-never-written.h5')
            except RuntimeError:
                calls.append('raised')
        return calls

    def test_a_finished_simulation_stops_its_job(self):
        pool = FakePool(JobState.DONE)
        calls = self.run_orchestrate(lambda *a, **k: None, pool)
        self.assertEqual(calls, ['tdc', ('stop', pool), 'reset'])

    def test_a_failed_simulation_still_stops_its_job(self):
        def fail(*a, **k):
            raise RuntimeError('the simulation broke')
        pool = FakePool(JobState.DONE)
        calls = self.run_orchestrate(fail, pool)
        self.assertEqual(calls, ['tdc', ('stop', pool), 'reset', 'raised'])

    def test_a_job_that_never_started_is_not_stopped_twice(self):
        """start_writer has already told the writer to stop it."""
        calls = self.run_orchestrate(lambda *a, **k: None, pool=None)
        self.assertEqual(calls, ['reset'])


if __name__ == '__main__':
    unittest.main()
