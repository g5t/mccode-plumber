import unittest


class SplitrunTestCase(unittest.TestCase):
    def test_parsing(self):
        from mccode_plumber.splitrun import make_parser
        parser = make_parser()
        args = parser.parse_args(['--broker', 'l:9092', '--source', 'm', '-n', '10000', 'inst.h5', 'a=1:4', 'b=2:5'])
        self.assertEqual(args.instrument, ['inst.h5'])
        self.assertEqual(args.broker, 'l:9092')
        self.assertEqual(args.source, 'm')
        self.assertEqual(args.ncount, [10000])
        self.assertEqual(args.parameters, ['a=1:4', 'b=2:5'])

    def test_mixed_order_throws(self):
        from mccode_plumber.splitrun import make_parser
        parser = make_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args(['inst.h5', '--broker', 'l:9092', '--source', 'm', '-n', '10000', 'a=1:4', 'b=2:5'])
        with self.assertRaises(SystemExit):
            parser.parse_args(['--broker', 'l:9092', '--source', 'm', 'inst.h5', '-n', '10000', 'a=1:4', 'b=2:5'])

    def test_sort_args(self):
        from mccode_plumber.splitrun import sort_args
        self.assertEqual(sort_args(['-n', '10000', 'inst.h5', 'a=1:4', 'b=2:5']), ['-n', '10000', 'inst.h5', 'a=1:4', 'b=2:5'])
        self.assertEqual(sort_args(['inst.h5', '-n', '10000', 'a=1:4', 'b=2:5']), ['-n', '10000', 'inst.h5', 'a=1:4', 'b=2:5'])

    def test_sorted_mixed_order_does_not_throw(self):
        from mccode_plumber.splitrun import make_parser, sort_args
        parser = make_parser()
        args = parser.parse_args(sort_args(['inst.h5', '--broker', 'l:9092', '--source', 'm', '-n', '10000', 'a=1:4', 'b=2:5']))
        self.assertEqual(args.instrument, ['inst.h5'])
        self.assertEqual(args.broker, 'l:9092')
        self.assertEqual(args.source, 'm')
        self.assertEqual(args.ncount, [10000])
        self.assertEqual(args.parameters, ['a=1:4', 'b=2:5'])


if __name__ == '__main__':
    unittest.main()
