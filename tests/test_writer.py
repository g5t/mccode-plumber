import unittest


class WriterTestCase(unittest.TestCase):
    def setUp(self):
        from json import dumps
        from mccode_antlr.loader import parse_mcstas_instr
        from mccode_to_kafka.writer import da00_dataarray_config, da00_variable_config
        t = da00_variable_config(name='t', label='monitor', unit='usec', axes=['t'], shape=[10], data_type='float64')
        ns = da00_dataarray_config(topic='monitor', source='mccode-to-kafka', variables=[t])
        instr = f"""DEFINE INSTRUMENT this_IS_NOT_BIFROST()
        TRACE
        COMPONENT origin = Arm() AT (0, 0, 0) ABSOLUTE
        COMPONENT source = Source_simple() AT (0, 0, 1) RELATIVE PREVIOUS
        COMPONENT monitor = TOF_monitor() AT (0, 0, 1) RELATIVE source
        METADATA "application/json" "nexus_structure_stream_data" %{{{dumps(ns)}%}}
        COMPONENT sample = Arm() AT (0, 0, 80) RELATIVE source
        END
        """
        self.instr = parse_mcstas_instr(instr)

    def test_parse(self):
        from mccode_plumber.writer import construct_writer_pv_dicts_from_parameters
        from mccode_plumber.writer import default_nexus_structure
        from moreniius.path_navigator import NexusStructureNavigator
        params = construct_writer_pv_dicts_from_parameters(self.instr.parameters, 'mcstas:', 'topic')
        self.assertEqual(len(params), 0)
        nav = NexusStructureNavigator(default_nexus_structure(self.instr))
        instrument = nav['/entry/instrument']
        self.assertTrue(all(instrument.exists(x) for x in ('origin', 'source', 'monitor')))

        data = instrument['monitor/data'].structure
        idx = [i for i, ch in enumerate(data['children']) if 'module' in ch and 'da00' == ch['module']]
        self.assertEqual(len(idx), 1)
        da00 = data['children'][idx[0]]
        self.assertEqual(len(da00.keys()), 2)
        self.assertEqual(da00['module'], 'da00')
        self.assertEqual(da00['config']['topic'], 'monitor')
        self.assertEqual(da00['config']['source'], 'mccode-to-kafka')


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
