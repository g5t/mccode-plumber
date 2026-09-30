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


def monitors_to_kafka_callback_for_topics(
        broker: str, sources: dict[str, list[str]],
        delete_after_sending: bool = True,
):
    """Send each monitor's histogram to the topic a NeXus structure puts it on.

    `sources` maps topic to the monitor names published there, as
    `orchestrate.sources_by_topic` groups them. A topic mapped to an empty list takes
    every histogram found, which is what a structure declaring no monitor stream falls
    back to.

    `send_histograms` takes a single topic per call, so several topics means several
    calls rather than one call carrying a topic per name. Removal is held back until
    every call has run and then done here by name: `send_histograms(remove=True)`
    deletes the files that call sent, so a monitor published on two topics would have
    its file deleted by the first call and be missing from the second.
    """
    from pathlib import Path as _Path
    from mccode_to_kafka.sender import send_histograms

    def callback(*args, **kwargs):
        root = kwargs.get('root', args[0] if args else None)
        root = _Path(root)
        # Resolve 'everything found' to actual names before sending, so that what gets
        # removed afterwards is exactly what got sent. Mirrors send_histograms' own
        # discovery, including a root that names a single .dat file.
        if root.is_file():
            found, root = [root.stem], root.parent
        else:
            found = [_Path(x).stem for x in root.glob('*.dat')]
        groups = {t: (list(names) if names else found) for t, names in sources.items()}

        for topic, names in groups.items():
            send_histograms(root, names=names, topic=topic, broker=broker, remove=False)

        if delete_after_sending:
            _remove_histograms(root, {n for names in groups.values() for n in names})

    return callback, {'dir': 'root'}


def _remove_histograms(root, names):
    """Delete the named histogram files, once every topic has had its chance to read them."""
    from mccode_to_kafka.sender import HistogramInfo
    for name in names:
        histogram = HistogramInfo(root, name)
        if histogram.exists:
            histogram.delete()


def _parameter_defaults(instr) -> dict[str, float]:
    """The numeric value of every instrument parameter that has one, by lower-cased name.

    The base a scan point is laid over: a chopper held fixed is not a scanned parameter and
    so appears nowhere in what the point hands over, but its disc is still turning.
    Publishing zero for it would be read downstream as a parked chopper.
    """
    return parameter_defaults(instr.parameters)


def parameter_defaults(parameters) -> dict[str, float]:
    """`_parameter_defaults`, for the parameters rather than the instrument holding them."""
    out = {}
    for parameter in parameters:
        expression = parameter.value
        if not getattr(expression, 'has_value', False):
            continue
        try:
            out[parameter.name.lower()] = float(expression.value)
        except (TypeError, ValueError):
            continue
    return out


def chopper_parameters_callback_with_arguments(instr, choppers, run_pv: str | None,
                                               logs=()):
    """A *pre*-point hook publishing what the point about to be traced is set to.

    Before, not after. `mp-tdc` free-runs on the pulse grid and reads the chopper PVs as it
    goes, so they have to describe the point that is about to be traced rather than the
    one that just finished -- the whole reason restage grew a pre-point hook. The same
    holds for any other simulated log: a sample rotation scanned point by point has to
    be on its PV while that point's events are being written.

    Each PV is put the value of the parameter that fills it, which is not always a
    parameter of the same name: a structure bound to a facility's names serves
    `mcstas:BIFRO-SpRot:MC-RotZ-01:Mtr.RBV` from `sample_rotation`. ``logs`` are the
    `orchestrate.SimulatedLog`s outside the choppers.

    Setting the run PV here rather than in `orchestrate` is deliberate: it keeps the
    crossings quiet through the primary MCPL stage, which produces no detector events and
    where these parameters still hold their defaults. The put is idempotent, so every point
    may safely repeat it. With no choppers there is no run PV to set.
    """
    from p4p.client.thread import Context

    targets = [pair for c in choppers for pair in c.served()]
    targets += [(log.source, log.parameter) for log in logs]
    defaults = _parameter_defaults(instr)
    missing: set[str] = set()
    context: list = []

    def callback(pars):
        if not context:
            context.append(Context('pva'))
        values = dict(defaults)
        values.update({str(k).lower(): v for k, v in pars.items()})
        for pv, parameter in targets:
            value = values.get(parameter.lower())
            if value is None:
                if parameter not in missing:
                    missing.add(parameter)
                    print(f'warning: no instrument parameter named {parameter!r}; '
                          f'{pv} will hold whatever it was last set to')
                continue
            context[0].put(pv, float(value))
        if run_pv is not None and choppers:
            context[0].put(run_pv, 1)

    return callback, {'pars': 'pars'}
