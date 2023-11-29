import unittest


class WriterTestCase(unittest.TestCase):
    def setUp(self):
        from json import dumps
        from mccode_antlr.loader import parse_mcstas_instr
        from mccode_to_kafka.writer import nexus_structure, edge
        monitor_data_stream = nexus_structure(topic='monitor', shape=[edge(10, 0.5, 10.5, 't', 'usec', 'monitor')])
        ns = dict(data=dict(type='dict', value=monitor_data_stream))
        instr = f"""DEFINE INSTRUMENT this_IS_NOT_BIFROST()
        TRACE
        COMPONENT origin = Arm() AT (0, 0, 0) ABSOLUTE
        COMPONENT source = Source_simple() AT (0, 0, 1) RELATIVE PREVIOUS
        COMPONENT monitor = TOF_monitor() AT (0, 0, 1) RELATIVE source
        METADATA "eniius_data" "json" %{{{dumps(ns)}%}}
        COMPONENT sample = Arm() AT (0, 0, 80) RELATIVE source
        END
        """
        self.instr = parse_mcstas_instr(instr)

    def test_parse(self):
        from mccode_plumber.writer import construct_writer_pv_dicts_from_parameters
        from mccode_plumber.writer import default_nexus_structure
        params = construct_writer_pv_dicts_from_parameters(self.instr.parameters, 'mcstas:', 'topic')
        self.assertEqual(len(params), 0)
        struct = default_nexus_structure(self.instr)

        self.assertEqual(len(struct['children']), 1)
        self.assertEqual(struct['children'][0]['name'], 'entry')
        self.assertEqual(struct['children'][0]['children'][0]['name'], 'instrument')
        self.assertEqual(struct['children'][0]['children'][0]['children'][1]['name'], 'origin')
        self.assertEqual(struct['children'][0]['children'][0]['children'][2]['name'], 'source')
        self.assertEqual(struct['children'][0]['children'][0]['children'][3]['name'], 'monitor')
        mon = struct['children'][0]['children'][0]['children'][3]
        self.assertEqual(len(mon['children']), 5)
        self.assertEqual(mon['children'][3]['module'], 'hs00')
        hs00= mon['children'][3]
        self.assertEqual(len(hs00.keys()), 3)  # Why does this have an attributes key?
        self.assertEqual(hs00['module'], 'hs00')
        self.assertEqual(hs00['config']['topic'], 'monitor')
        self.assertEqual(hs00['config']['source'], 'mccode-to-kafka')
        self.assertEqual(hs00['config']['data_type'], 'double')
        self.assertEqual(hs00['config']['error_type'], 'double')
        self.assertEqual(hs00['config']['edge_type'], 'double')



if __name__ == '__main__':
    unittest.main()
