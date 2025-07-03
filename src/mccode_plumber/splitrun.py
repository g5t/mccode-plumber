def make_parser():
    from mccode_plumber import __version__
    from restage.splitrun import make_splitrun_parser
    parser = make_splitrun_parser()
    parser.prog = 'mp-splitrun'
    parser.add_argument('--broker', type=str, help='The Kafka broker to send monitors to', default=None)
    parser.add_argument('--source', type=str, help='The Kafka source name to use for monitors', default=None)
    parser.add_argument('--topic', type=str, help='The Kafka topic name(s) to use for monitors', default=None, action='append')
    parser.add_argument('-v', '--version', action='version', version=__version__)
    return parser


def monitors_to_kafka_callback_with_arguments(broker: str, source: str, topics: list[str]):
    from mccode_to_kafka.sender import send_histograms

    partial_kwargs = {'broker': broker, 'source': source}
    if topics is not None and len(topics) > 0:
        partial_kwargs['names'] = topics

    def callback(*args, **kwargs):
        return send_histograms(*args, **partial_kwargs, **kwargs)

    return callback, {'dir': 'root'}


def main():
    from .mccode import get_mcstas_instr
    from restage.splitrun import splitrun_args, parse_splitrun
    args, parameters, precision = parse_splitrun(make_parser())
    instr = get_mcstas_instr(args.instrument)
    callback, callback_args = monitors_to_kafka_callback_with_arguments(args.broker, args.source, args.topic)
    return splitrun_args(instr, parameters, precision, args, callback=callback, callback_arguments=callback_args)
