from restage.splitrun import splitrun


def make_parser():
    from restage.splitrun import make_splitrun_parser
    parser = make_splitrun_parser()
    parser.add_argument('--broker', type=str, help='The Kafka broker to send monitors to', default=None)
    parser.add_argument('--source', type=str, help='The Kafka source name to use for monitors', default=None)
    return parser


def parse_args():
    from restage.splitrun import parse_splitrun_parameters, parse_splitrun_precision
    args = make_parser().parse_args()
    parameters = parse_splitrun_parameters(args.parameters)
    precision = parse_splitrun_precision(args.P)
    return args, parameters, precision


def monitors_to_kafka_callback_with_arguments(broker: str, source: str):
    from functools import partial
    from mccode_to_kafka.sender import send_histograms
    return partial(send_histograms, broker=broker, source=source), {'dir': 'root'}


def main():
    from mccode_antlr.loader import load_mcstas_instr
    args, parameters, precision = parse_args()
    instr = load_mcstas_instr(args.isntrument[0])
    callback, callback_args = monitors_to_kafka_callback_with_arguments(args.broker[0], args.source[0])
    return splitrun(instr, parameters, precision, split_at=args.split_at[0], grid=args.mesh,
                    seed=args.seed[0] if args.seed is not None else None,
                    ncount=args.ncount[0] if args.ncount is not None else None,
                    out_dir=args.dir[0] if args.dir is not None else None,
                    trace=args.trace,
                    gravitation=args.gravitation,
                    bufsiz=args.bufsiz[0] if args.bufsiz is not None else None,
                    format=args.format[0] if args.format is not None else None,
                    minimum_particle_count=args.nmin[0] if args.nmin is not None else None,
                    maximum_particle_count=args.nmax[0] if args.nmax is not None else None,
                    dry_run=args.dryrun,
                    callback=callback,
                    callback_arguments=callback_args,
                    )
