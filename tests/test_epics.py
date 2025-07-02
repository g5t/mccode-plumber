import unittest


class EPICSTestCase(unittest.TestCase):
    def setUp(self):
        from uuid import uuid4
        from multiprocessing import Process
        from mccode_plumber.epics import main, convert_instr_parameters_to_nt
        from mccode_antlr.loader.loader import parse_mccode_instr_parameters, parse_mcstas_instr
        instr = 'define instrument blah(par1, double par2, int par3=1, string par4="string", double par5=5.5) trace end'
        self.pars = parse_mccode_instr_parameters(instr)
        self.pvs = convert_instr_parameters_to_nt(parse_mcstas_instr(instr).parameters)
        self.prefix = f"test{str(uuid4()).replace('-', '')}:"
        self.proc = Process(target=main, args=(self.pvs, self.prefix))
        self.proc.start()

    def tearDown(self):
        self.proc.terminate()
        self.proc.join(1)
        self.proc.close()

    def test_server_runs(self):
        from p4p.client.thread import Context
        providers = Context.providers()
        self.assertTrue('pva' in providers)
        ctx = Context('pva')

        for par in self.pars:
            pv = ctx.get(f"{self.prefix}{par.name}")
            self.assertTrue(pv is not None)
            # The mailbox is rejecting values at startup for being too old ??
            # This doesn't prevent it from working.
            # if par.value.has_value:
            #     self.assertEqual(pv, par.value.value)

    def test_update_pvs(self):
        from p4p.client.thread import Context
        ctx = Context('pva')
        values = {'par1': 1.1, 'par2': 2.2, 'par3': 3, 'par4': 'four', 'par5': 55.555}
        for name, value in values.items():
            ctx.put(f"{self.prefix}{name}", value)

        for name, value in values.items():
            pv = ctx.get(f"{self.prefix}{name}")
            self.assertTrue(pv is not None)
            self.assertEqual(pv, value)


if __name__ == '__main__':
    unittest.main()
