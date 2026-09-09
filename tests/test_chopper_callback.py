"""The pre-point hook that says what the choppers are about to be doing.

Before the point, not after: `mp-tdc` free-runs on the pulse grid and reads these PVs as
it goes, so they have to describe the point about to be traced.
"""
import unittest
from unittest.mock import patch

from mccode_plumber.conductor import Chopper
from mccode_plumber.splitrun import (
    _parameter_defaults, chopper_parameters_callback_with_arguments,
)

DISC = Chopper(name='psc1', tdc='psc1_tdc', speed='psc1speed', delay='psc1delay',
               park='psc1park')


class _FakeExpr:
    def __init__(self, value, has_value=True):
        self.value = value
        self.has_value = has_value


class _FakeParameter:
    def __init__(self, name, value, has_value=True):
        self.name = name
        self.value = _FakeExpr(value, has_value)


class _FakeInstr:
    def __init__(self, *parameters):
        self.parameters = parameters


class _FakeContext:
    def __init__(self):
        self.puts = []

    def put(self, name, value):
        self.puts.append((name, value))


def run(instr, pars, choppers=(DISC,), run_pv='tdc_run'):
    context = _FakeContext()
    with patch('p4p.client.thread.Context', lambda *a, **k: context):
        callback, mapping = chopper_parameters_callback_with_arguments(
            instr, list(choppers), run_pv)
        callback(pars=pars)
    return context.puts, mapping


class ParameterDefaultsTest(unittest.TestCase):

    def test_numeric_defaults_are_taken_by_lower_cased_name(self):
        instr = _FakeInstr(_FakeParameter('PSC1speed', 14.0))
        self.assertEqual(_parameter_defaults(instr), {'psc1speed': 14.0})

    def test_a_parameter_with_no_default_is_skipped(self):
        instr = _FakeInstr(_FakeParameter('a', None, has_value=False))
        self.assertEqual(_parameter_defaults(instr), {})

    def test_a_string_parameter_is_skipped(self):
        """`mcpl_filename` is not a number and there is nothing to publish for it."""
        instr = _FakeInstr(_FakeParameter('mcpl_filename', 'somewhere.mcpl'))
        self.assertEqual(_parameter_defaults(instr), {})


class CallbackTest(unittest.TestCase):

    def setUp(self):
        self.instr = _FakeInstr(
            _FakeParameter('psc1speed', 14.0),
            _FakeParameter('psc1delay', 0.005),
            _FakeParameter('psc1park', 0.0),
        )

    def test_it_asks_restage_for_the_translated_parameters(self):
        _, mapping = run(self.instr, {})
        self.assertEqual(mapping, {'pars': 'pars'})

    def test_a_scanned_value_wins_over_the_default(self):
        puts, _ = run(self.instr, {'psc1delay': 0.017})
        self.assertIn(('psc1delay', 0.017), puts)

    def test_a_fixed_chopper_is_published_from_its_default(self):
        """It is not a scanned parameter, so it appears nowhere in what the point hands
        over -- but the disc is still turning. Publishing zero would read as parked."""
        puts, _ = run(self.instr, {'psc1delay': 0.017})
        self.assertIn(('psc1speed', 14.0), puts)

    def test_scan_names_are_matched_case_insensitively(self):
        """`parameters_to_scan` lower-cases them on the way through."""
        instr = _FakeInstr(_FakeParameter('a', 1.0))
        puts, _ = run(instr, {'PSC1SPEED': 196.0})
        self.assertIn(('psc1speed', 196.0), puts)

    def test_the_run_is_declared_last(self):
        """The values first, then the flag, so the first published pulse already has
        this point's numbers behind it."""
        puts, _ = run(self.instr, {})
        self.assertEqual(puts[-1], ('tdc_run', 1))

    def test_an_unknown_parameter_is_reported_rather_than_published_as_zero(self):
        instr = _FakeInstr(_FakeParameter('psc1speed', 14.0))
        with patch('builtins.print') as printed:
            puts, _ = run(instr, {})
        self.assertEqual([n for n, _ in puts], ['psc1speed', 'tdc_run'])
        said = ' '.join(str(c) for c in printed.call_args_list)
        self.assertIn('psc1delay', said)


if __name__ == '__main__':
    unittest.main()
