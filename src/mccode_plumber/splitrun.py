def make_parser():
    from restage.splitrun import make_splitrun_parser
    parser = make_splitrun_parser()
    parser.prog = 'mp-splitrun'
    parser.add_argument('--broker', type=str, help='The Kafka broker to send monitors to', default=None)
    parser.add_argument('--source', type=str, help='The Kafka source name to use for monitors', default=None)
    return parser


def monitors_to_kafka_callback_with_arguments(broker: str, source: str):
    from functools import partial
    from mccode_to_kafka.sender import send_histograms

    def callback(*args, **kwargs):
        print(f'monitors to kafka callback called with {args} and {kwargs}')
        return send_histograms(*args, broker=broker, source=source, **kwargs)

    # return partial(send_histograms, broker=broker, source=source), {'dir': 'root'}
    return callback, {'dir': 'root'}


def main():
    from .mccode import get_mcstas_instr
    from restage.splitrun import splitrun_args, parse_splitrun
    args, parameters, precision = parse_splitrun(make_parser())
    instr = get_mcstas_instr(args.instrument[0])
    callback, callback_args = monitors_to_kafka_callback_with_arguments(args.broker, args.source)
    return splitrun_args(instr, parameters, precision, args, callback=callback, callback_arguments=callback_args)
