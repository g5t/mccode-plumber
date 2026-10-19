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
        broker: str, sources: dict[str, dict[str, str] | list[str]],
        delete_after_sending: bool = True,
):
    """Send each monitor's histogram to the topic a NeXus structure puts it on.

    `sources` maps topic to the monitors published there, each as histogram name to
    Kafka source name, as `orchestrate.monitor_sources_and_topics` reads them from the
    structure. A list of names publishes each under its own name. A topic mapped to
    nothing takes every histogram found, which is what a structure declaring no monitor
    stream falls back to.

    Each histogram gets its own sink, because the source is a property of the sink.
    Removal is held back until every topic has sent: a monitor published on two topics
    must still be there for the second.
    """
    from pathlib import Path as _Path

    def callback(*args, **kwargs):
        root = kwargs.get('root', args[0] if args else None)
        root = _Path(root)
        if root.is_file():
            found, root = [root.stem], root.parent
        else:
            found = [_Path(x).stem for x in root.glob('*.dat')]
        groups = {t: (dict(zip(names, names)) if isinstance(names, list) else dict(names))
                  or {n: n for n in found} for t, names in sources.items()}

        for topic, names in groups.items():
            for name, source in names.items():
                _send_histogram(root, name, topic, source, broker)

        if delete_after_sending:
            _remove_histograms(root, {n for names in groups.values() for n in names})

    return callback, {'dir': 'root'}


def _send_histogram(root, name, topic, source, broker):
    """Publish one histogram file, if the simulation wrote it, under `source`."""
    from mccode_to_kafka.sender import HistogramInfo
    from mccode_to_kafka.histogram import create_histogram_sink
    histogram = HistogramInfo(root, name)
    if histogram.exists:
        sink = create_histogram_sink({'data_brokers': [broker], 'source': source}, {})
        sink.send_histogram(topic, histogram.data, information=histogram.message)


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


def _numeric_parameters(instr) -> dict[str, type]:
    """Every instrument parameter a PV can carry, and whether it is an int or a float.

    Strings are left out: `f144` carries numbers only, which is also why the forwarder is
    never told about them.
    """
    from mccode_antlr.common.expression import DataType
    kinds = {DataType.int: int, DataType.float: float}
    out = {}
    for parameter in instr.parameters:
        kind = kinds.get(getattr(parameter.value, 'data_type', None))
        if kind is not None:
            out[parameter.name] = kind
    return out


class MissingChopperParameter(ValueError):
    """A disc's crossings would follow from a parameter the instrument does not have."""


def require_chopper_parameters(instr, choppers) -> None:
    """Refuse a disc whose speed or delays are not instrument parameters.

    A disc's top-dead-centre crossings are computed from its speed and delay, and the
    only values for them anyone has are the instrument parameters a run sets -- recorded
    per point in a collector file, put to `mp-tdc`'s inputs before each point. Named in
    the structure but missing from the instrument, the PV keeps whatever it last held,
    and every crossing comes out wrong without a word. The park angle is not needed for
    a crossing, so a missing one is left to the warning the hook gives.
    """
    declared = {parameter.name.lower() for parameter in instr.parameters}
    missing = [f'{chopper.name} {which} ({chopper.parameter(which)})'
               for chopper in choppers for which in chopper.inputs() if which != 'park'
               and chopper.parameter(which).lower() not in declared]
    if missing:
        raise MissingChopperParameter(
            'Top-dead-centre times follow from a disc\'s speed and delay, which must be '
            'instrument parameters; the instrument has none for: ' + ', '.join(missing))


def parameter_pvs_callback_with_arguments(instr, choppers, run_pv: str | None,
                                           logs=(), prefix: str | None = None):
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

    With ``prefix``, every numeric instrument parameter is also put to ``{prefix}{name}``:
    the mailbox PVs `/entry/parameters` is filled from. That is what the `UpdateEPICS`
    component used to do from inside the simulation; doing it here puts every value in
    place before the point starts, and leaves the instrument free of an EPICS call-out.

    Setting the run PV here rather than in `orchestrate` is deliberate: it keeps the
    crossings quiet through the primary MCPL stage, which produces no detector events and
    where these parameters still hold their defaults. The put is idempotent, so every point
    may safely repeat it. With no choppers there is no run PV to set.
    """
    from mccode_plumber.pva import context as loopback_context

    targets = [pair for c in choppers for pair in c.served()]
    targets += [(log.source, log.parameter) for log in logs]
    kinds = _numeric_parameters(instr) if prefix is not None else {}
    targets += [(f'{prefix}{name}', name) for name in kinds]
    # One put per PV: an unbound log's source is the mailbox's own name for its parameter.
    targets = list(dict.fromkeys(targets))
    defaults = _parameter_defaults(instr)
    missing: set[str] = set()
    context: list = []

    def callback(pars):
        if not context:
            context.append(loopback_context())
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
            kind = kinds.get(parameter, float) if pv == f'{prefix}{parameter}' else float
            context[0].put(pv, kind(value))
        if run_pv is not None and choppers:
            context[0].put(run_pv, 1)

    return callback, {'pars': 'pars'}


#: The name this had while it published only chopper values.
chopper_parameters_callback_with_arguments = parameter_pvs_callback_with_arguments
