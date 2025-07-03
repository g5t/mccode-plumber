import unittest

# FIXME These tests exercise internal details of the splitrun parser,
#       which we don't depend on since we _always_ call `parse_splitrun({our parser}`)
#       which rearranges ArgParse's results before returning
#       (args, parameters, precision) therefore, directly checking the ArgParse result
#       is fraught and these tests may fail at any internal change in restage
# TODO  A future restage release will include an `args_fixup` function that can be used
#       to get the correct output args of `parse_splitrun` in our testing here.

class SplitrunTestCase(unittest.TestCase):
    def test_parsing(self):
        from mccode_plumber.splitrun import make_parser
        parser = make_parser()
        args = parser.parse_args(['--broker', 'l:9092', '--source', 'm', '-n', '10000', 'inst.h5', '--', 'a=1:4', 'b=2:5'])
        self.assertEqual(args.instrument, 'inst.h5')
        self.assertEqual(args.broker, 'l:9092')
        self.assertEqual(args.source, 'm')
        self.assertEqual(args.ncount, (None, 10000, None))
        self.assertEqual(args.parameters, ['a=1:4', 'b=2:5'])
        self.assertFalse(args.parallel)

    def test_mixed_order_throws(self):
        from mccode_plumber.splitrun import make_parser
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
        from mccode_plumber.splitrun import make_parser
        from mccode_antlr.run.runner import sort_args
        parser = make_parser()
        args = parser.parse_args(sort_args(['inst.h5', '--broker', 'www.github.com:9093', '--source', 'dev/null',
                                            '-n', '123', '--parallel', '--', 'a=1:4', 'b=2:5']))
        self.assertEqual(args.instrument, 'inst.h5')
        self.assertEqual(args.broker, 'www.github.com:9093')
        self.assertEqual(args.source, 'dev/null')
        self.assertEqual(args.ncount, (None, 123, None))
        self.assertEqual(args.parameters, ['a=1:4', 'b=2:5'])
        self.assertTrue(args.parallel)


if __name__ == '__main__':
    unittest.main()
