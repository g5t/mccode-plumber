from restage.splitrun import splitrun


def make_parser():
    from restage.splitrun import make_splitrun_parser
    parser = make_splitrun_parser()
    parser.add_argument('--broker', type=str, help='The Kafka broker to send monitors to', default=None)
    parser.add_argument('--source', type=str, help='The Kafka source name to use for monitors', default=None)
    return parser


def sort_args(args: list[str]) -> list[str]:
    """Take the list of arguments and sort them into the correct order for splitrun"""
    # TODO this is a bit of a hack, but it works for now
    # TODO this should be done in splitrun
    first, last = [], []
    k = 0
    while k < len(args):
        if args[k].startswith('-'):
            first.append(args[k])
            k += 1
            if '=' not in first[-1] and k < len(args) and not args[k].startswith('-') and '=' not in args[k]:
                first.append(args[k])
                k += 1
        else:
            last.append(args[k])
            k += 1
    return first + last


def parse_args():
    from restage.range import parse_scan_parameters
    from restage.splitrun import parse_splitrun_precision
    import sys
    sys.argv[1:] = sort_args(sys.argv[1:])

    args = make_parser().parse_args()

    parameters = parse_scan_parameters(args.parameters)
    precision = parse_splitrun_precision(args.P)
    return args, parameters, precision


def monitors_to_kafka_callback_with_arguments(broker: str, source: str):
    from functools import partial
    from mccode_to_kafka.sender import send_histograms
    return partial(send_histograms, broker=broker, source=source), {'dir': 'root'}


def main():
    from .mccode import get_mcstas_instr
    args, parameters, precision = parse_args()
    instr = get_mcstas_instr(args.instrument[0])
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
