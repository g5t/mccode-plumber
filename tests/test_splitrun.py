import unittest
from mccode_plumber.splitrun import make_parser
from restage.splitrun import args_fixup


class SplitrunTestCase(unittest.TestCase):
    def test_parsing(self):
        parser = make_parser()
        args = args_fixup(parser.parse_args(['--broker', 'l:9092', '--source', 'm', '-n', '10000', 'inst.h5', '--', 'a=1:4', 'b=2:5']))
        self.assertEqual(args.instrument, 'inst.h5')
        self.assertEqual(args.broker, 'l:9092')
        self.assertEqual(args.source, 'm')
        self.assertEqual(args.ncount, 10000)
        self.assertEqual(args.parameters, ['a=1:4', 'b=2:5'])
        self.assertFalse(args.parallel)

    def test_mixed_order_throws(self):
        parser = make_parser()
        parser.prog = "{{This failed before but works now? Why did it stop throwing?}}"
        pa = parser.parse_args
        # These also output usage information to stdout -- don't be surprised by the 'extra' test output.
        pa(['inst.h5', '--broker', 'l:9092', '--source', 'm', '-n', '10000',
            'a=1:4', 'b=2:5'
            ])
        pa(['--broker', 'l:9092', '--source', 'm', 'inst.h5', '-n', '10000',
            'a=1:4', 'b=2:5'
            ])

    def test_sort_args(self):
        from mccode_antlr.run.runner import sort_args
        self.assertEqual(sort_args(['-n', '10000', 'inst.h5', 'a=1:4', 'b=2:5']), ['-n', '10000', 'inst.h5', 'a=1:4', 'b=2:5'])
        self.assertEqual(sort_args(['inst.h5', '-n', '10000', 'a=1:4', 'b=2:5']), ['-n', '10000', 'inst.h5', 'a=1:4', 'b=2:5'])

    def test_sorted_mixed_order_does_not_throw(self):
        from mccode_antlr.run.runner import sort_args
        parser = make_parser()
        args = args_fixup(parser.parse_args(sort_args(['inst.h5', '--broker', 'www.github.com:9093', '--source', 'dev/null',
                                            '-n', '123', '--parallel', '--', 'a=1:4', 'b=2:5'])))
        self.assertEqual(args.instrument, 'inst.h5')
        self.assertEqual(args.broker, 'www.github.com:9093')
        self.assertEqual(args.source, 'dev/null')
        self.assertEqual(args.ncount, 123)
        self.assertEqual(args.parameters, ['a=1:4', 'b=2:5'])
        self.assertTrue(args.parallel)

    def test_ncount_limits(self):
        args = args_fixup(make_parser().parse_args([
            'inst.json', '--broker', 'l:9092', '--source', 'm',
            '-n', '1M', '--nmin', '100k', '--nmax', '1G'
        ]))
        self.assertEqual(args.ncount, 10**6)
        self.assertEqual(args.nmin, 100000)
        self.assertEqual(args.nmax, 10**9)
        args = args_fixup(make_parser().parse_args([
            'inst.json', '--broker', 'l:9092', '--source', 'm', '-n', '100k]1M[1G'
        ]))
        self.assertEqual(args.ncount, 10 ** 6)
        self.assertEqual(args.nmin, 100000)
        self.assertEqual(args.nmax, 10 ** 9)
        args = args_fixup(make_parser().parse_args([
            'inst.json', '--broker', 'l:9092', '--source', 'm', '-n', '1-1Ki+Mi'
        ]))
        self.assertEqual(args.ncount, 2**10)
        self.assertEqual(args.nmin, 1)
        self.assertEqual(args.nmax, 2**20)


if __name__ == '__main__':
    unittest.main()
