import unittest


class WriterTestCase(unittest.TestCase):
    """The structure is given -- plumber extends it with the parameters and a title."""

    def setUp(self):
        from mccode_antlr.loader import parse_mcstas_instr
        instr = f"""DEFINE INSTRUMENT with_a_knob(double speed/"Hz"=14)
        TRACE
        COMPONENT origin = Arm() AT (0, 0, 0) ABSOLUTE
        COMPONENT source = Source_simple() AT (0, 0, 1) RELATIVE PREVIOUS
        END
        """
        self.instr = parse_mcstas_instr(instr)
        self.structure = {'children': [{'name': 'entry', 'type': 'group', 'attributes': [
            {'name': 'NX_class', 'dtype': 'string', 'values': 'NXentry'}], 'children': [
            {'name': 'instrument', 'type': 'group', 'children': [], 'attributes': [
                {'name': 'NX_class', 'dtype': 'string', 'values': 'NXinstrument'}]}]}]}

    def define(self, **sources):
        from tempfile import TemporaryDirectory
        from pathlib import Path
        from mccode_antlr.io.json import save_json
        from mccode_plumber.writer import (construct_writer_pv_dicts_from_parameters,
                                           make_define_nexus_structure)
        pvs = construct_writer_pv_dicts_from_parameters(self.instr.parameters, 'mcstas:', 'topic')
        with TemporaryDirectory() as tmp:
            instr = Path(tmp) / 'with_a_knob.instr.json'
            save_json(self.instr, instr)
            return make_define_nexus_structure()(instr, pvs, title='a title', **sources)

    def test_a_given_structure_is_extended(self):
        from json import dump
        from tempfile import TemporaryDirectory
        from pathlib import Path
        with TemporaryDirectory() as tmp:
            file = Path(tmp) / 'structure.json'
            with open(file, 'w') as f:
                dump(self.structure, f)
            structure = self.define(file=file)
        entry = structure['children'][0]
        names = [c.get('name') or c.get('config', {}).get('name') for c in entry['children']]
        self.assertIn('instrument', names)
        self.assertIn('title', names)
        parameters = next(c for c in entry['children'] if c.get('name') == 'parameters')
        self.assertEqual([c['name'] for c in parameters['children']], ['speed'])

    def test_no_structure_is_an_error(self):
        """Not one invented from the instrument: that was moreniius's job, and it is gone."""
        with self.assertRaisesRegex(ValueError, 'No NeXus structure given'):
            self.define()


class WriterUnitsTestCase(unittest.TestCase):
    def setUp(self):
        from mccode_antlr.loader import parse_mcstas_instr
        instr = f"""DEFINE INSTRUMENT with_logs(double a/"Hz", b/"m", int c, string d)
        TRACE
        COMPONENT origin = Arm() AT (0, 0, 0) ABSOLUTE
        COMPONENT source = Source_simple() AT (0, 0, 1) RELATIVE PREVIOUS
        COMPONENT sample = Arm() AT (0, 0, 80) RELATIVE source
        END
        """
        self.instr = parse_mcstas_instr(instr)

    def test_parse(self):
        from mccode_plumber.writer import construct_writer_pv_dicts_from_parameters
        params = construct_writer_pv_dicts_from_parameters(self.instr.parameters, 'mcstas:', 'topic')
        # Only non-string valued parameters should be extracted since f144 only
        # supports numeric-valued data
        self.assertEqual(len(params), 3)
        for p, x in zip(params, [('a', 'Hz'), ('b', 'm'), ('c', None)]):
            self.assertEqual(p['name'], x[0])
            self.assertEqual(p['unit'], x[1])
            self.assertEqual(p['module'], 'f144')


if __name__ == '__main__':
    unittest.main()
