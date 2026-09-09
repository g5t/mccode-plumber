from __future__ import annotations

from typing import Union


def make_parser():
    from mccode_plumber import __version__
    from restage.splitrun import make_splitrun_parser
    parser = make_splitrun_parser()
    parser.prog = 'mp-splitrun'
    parser.add_argument('--broker', type=str, help='The Kafka broker to send monitors to', default=None)
    parser.add_argument('--source', type=str, help='The Kafka source name to use for monitors', default=None)
    parser.add_argument('--topic', type=str, help='The Kafka topic name to use for monitors', default=None)
    parser.add_argument('--names', type=str, help='The monitor name(s) to send to Kafka', default=None, action='append')
    parser.add_argument('-v', '--version', action='version', version=__version__)
    return parser


def monitors_to_kafka_callback_with_arguments(
        broker: str, topic: str | None, source: str | None, names: list[str] | None,
        delete_after_sending: bool = True,
):
    from mccode_to_kafka.sender import send_histograms

    partial_kwargs: dict[str, Union[str,list[str], bool]] = {
        'broker': broker,
        'remove': delete_after_sending,
    }
    if topic is not None and source is not None and names is not None and len(names) > 1:
        raise ValueError("Cannot specify both topic/source and multiple names simultaneously.")

    if topic is not None:
        partial_kwargs['topic'] = topic
    if source is not None:
        partial_kwargs['source'] = source
    if names is not None and len(names) > 0:
        partial_kwargs['names'] = names

    def callback(*args, **kwargs):
        return send_histograms(*args, **partial_kwargs, **kwargs)

    return callback, {'dir': 'root'}


def _parameter_defaults(instr) -> dict[str, float]:
    """The numeric value of every instrument parameter that has one, by lower-cased name.

    The base a scan point is laid over: a chopper held fixed is not a scanned parameter and
    so appears nowhere in what the point hands over, but its disc is still turning.
    Publishing zero for it would be read downstream as a parked chopper.
    """
    out = {}
    for parameter in instr.parameters:
        expression = parameter.value
        if not getattr(expression, 'has_value', False):
            continue
        try:
            out[parameter.name.lower()] = float(expression.value)
        except (TypeError, ValueError):
            continue
    return out


def chopper_parameters_callback_with_arguments(instr, choppers, run_pv: str):
    """A *pre*-point hook publishing what the choppers are about to be doing.

    Before, not after. `mp-tdc` free-runs on the pulse grid and reads these PVs as it goes,
    so they have to describe the point that is about to be traced rather than the one that
    just finished -- the whole reason restage grew a pre-point hook.

    Setting the run PV here rather than in `orchestrate` is deliberate: it keeps the
    crossings quiet through the primary MCPL stage, which produces no detector events and
    where these parameters still hold their defaults. The put is idempotent, so every point
    may safely repeat it.
    """
    from p4p.client.thread import Context

    names = [n for c in choppers for n in (c.speed, c.delay, c.park) if n]
    defaults = _parameter_defaults(instr)
    missing: set[str] = set()
    context: list = []

    def callback(pars):
        if not context:
            context.append(Context('pva'))
        values = dict(defaults)
        values.update({str(k).lower(): v for k, v in pars.items()})
        for name in names:
            value = values.get(name.lower())
            if value is None:
                if name not in missing:
                    missing.add(name)
                    print(f'warning: no instrument parameter named {name!r}; its '
                          f'chopper log will hold whatever the PV was last set to')
                continue
            context[0].put(name, float(value))
        context[0].put(run_pv, 1)

    return callback, {'pars': 'pars'}


def main():
    from .mccode import get_mcstas_instr
    from restage.splitrun import splitrun_args, parse_splitrun
    parser = make_parser()
    parser.add_argument('--keep-after-send', action='store_true', help='Keep after sending histograms', default=False)
    args, parameters, precision = parse_splitrun(parser)
    instr = get_mcstas_instr(args.instrument)
    callback, callback_args = monitors_to_kafka_callback_with_arguments(
        broker=args.broker,
        topic=args.topic,
        source=args.source,
        names=args.names,
        delete_after_sending=not args.keep_after_send
    )
    return splitrun_args(instr, parameters, precision, args, callback=callback, callback_arguments=callback_args)
