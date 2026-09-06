"""Replay a recorded simulation into EPICS, choppers included.

`mcstas-readout-master` replays a collector file: it sends detector events to the event
formation units as UDP packets, and hands each scan point's instrument-parameter values
to a `ParameterPublisher`. It deliberately stops there -- the EPICS transport lives here
so that library stays dependency-light.

What this adds beyond passing values through is the chopper half. An ESS `NXdisk_chopper`
wants a stream of top-dead-centre times, and a simulation does not record them: it
records the two numbers they follow from, the disc's speed and its delay. So the times
are computed here, from those parameters and the pulse the replayer just started, and
written to the chopper's TDC channel as a vector of absolute nanoseconds. The external
forwarder turns that PV into `tdct` on Kafka; nothing here serialises a flatbuffer.

The pulse is the whole point. A top-dead-centre time means nothing on its own -- it is
measured *from* a pulse -- so the same instant that stamps the TDC vector is also
published as the reference sample, and every timestamp in the resulting file shares it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

#: One nanosecond in seconds, spelled out because every conversion here is to integer
#: nanoseconds and a stray float second is the kind of error that survives review.
NS = 1_000_000_000


@dataclass(frozen=True)
class Chopper:
    """One disc, and where its numbers live.

    ``speed`` and ``delay`` name *instrument parameters* -- what the simulation recorded
    -- while ``tdc`` names the *PV* the computed timestamps are written to. The two are
    deliberately different namespaces: one is what McStas called the knob, the other is
    what the control system calls the channel.
    """
    name: str
    tdc: str
    speed: str
    delay: str
    park: Optional[str] = None

    def crossings(self, pulse_ns: int, values: dict[str, float]) -> list[int]:
        """When the disc's mark passes, over one pulse, as absolute nanoseconds.

        The disc turns at ``speed`` and its mark reaches the beam ``delay`` after the
        pulse, so the crossings are ``delay + k/|speed|`` -- the same arithmetic the
        emitted McStas already does to offset each opening from the disc's own delay.

        A stationary disc has no crossings at all. It is not an error and not an empty
        measurement: a parked chopper genuinely never triggers its sensor, and saying so
        by publishing nothing is what the rest of the toolchain already does with a
        zero-speed disc.

        The crossings are *not* clipped to the pulse. With a large delay and a fast disc
        the later ones legitimately fall after the next pulse begins; truncating them
        would silently drop real events.
        """
        speed = float(values.get(self.speed, 0.0))
        if not speed:
            return []
        delay_ns = int(round(float(values.get(self.delay, 0.0)) * NS))
        period_ns = int(round(NS / abs(speed)))
        if period_ns <= 0:
            return []
        turns = max(1, int(round(NS / 14.0 / period_ns)))
        return [pulse_ns + delay_ns + k * period_ns for k in range(turns)]


@dataclass
class ChopperPublisher:
    """A `ParameterPublisher` that drives an EPICS mailbox, choppers and all.

    Held together by one rule: every timestamp it writes comes from the pulse instant the
    replayer reports, never from a clock of its own. A value stamped here with
    `time.time_ns()` would land before the pulse it belongs to, because the replayer
    sleeps to the next grid tick *after* the parameters are published -- at 14 Hz that is
    up to 71 ms, a whole chopper revolution.
    """
    prefix: str = 'mcstas:'
    choppers: tuple[Chopper, ...] = ()
    #: The PV carrying one sample per pulse of something like proton intensity on
    #: target. Its timestamps are the reference every other timestamp is measured from.
    pulse_pv: str = 'pulse'
    #: What to publish as that sample. A simulation has no accelerator, so the value is
    #: a stand-in; the timestamp is the part that matters.
    pulse_value: float = 1.0
    context: object = None

    _values: dict = field(default_factory=dict, init=False)
    _sequence: dict = field(default_factory=dict, init=False)

    def __post_init__(self):
        if self.context is None:
            from p4p.client.thread import Context
            self.context = Context('pva')

    # -- the ParameterPublisher contract ------------------------------------------

    def publish(self, point: int, name: str, value: str, unit: str | None) -> None:
        """One parameter value for one point.

        Kept as well as forwarded: a chopper's speed and delay arrive this way, and the
        crossings cannot be computed until the pulse is known, which is later.
        """
        self._values[name] = value
        self._put(f'{self.prefix}{name}', value)

    def point_ready(self, point: int) -> None:
        """All of this point's parameters have been published."""

    def pulse_ready(self, point: int, pulse_ns: int) -> None:
        """The replayer has started a pulse at ``pulse_ns``; everything hangs off it."""
        self._put(self.pulse_pv, self.pulse_value)
        numbers = self._numeric_values()
        for chopper in self.choppers:
            times = chopper.crossings(pulse_ns, numbers)
            if times:
                self._put_timestamps(chopper.tdc, times)

    # -- putting values ------------------------------------------------------------

    def _numeric_values(self) -> dict[str, float]:
        """The recorded parameters that are numbers. Values arrive as strings."""
        out = {}
        for name, value in self._values.items():
            try:
                out[name] = float(value)
            except (TypeError, ValueError):
                continue
        return out

    def _put(self, address: str, value) -> None:
        from .epics import parse_like
        if isinstance(value, str):
            current = self.context.get(address, throw=False)
            if isinstance(current, Exception):
                return
            value = parse_like(current, value)
        self.context.put(address, value)

    def _put_timestamps(self, address: str, times: list[int]) -> None:
        import numpy as np
        self.context.put(address, np.asarray(times, dtype=np.uint64))
        self._sequence[address] = self._sequence.get(address, 0) + 1


def chopper_streams(choppers, topic: str, prefix: str = ''):
    """Forwarder stream declarations for these choppers."""
    from .forwarder import chopper_partial_streams
    return chopper_partial_streams(
        [dict(tdc=c.tdc, values=[]) for c in choppers], topic, prefix)


def chopper_pv_strings(choppers, pulse_pv: str = 'pulse'):
    """Mailbox PV declarations, in `mp-epics-strings` form.

    A TDC channel is `aL` -- an array of unsigned 64-bit integers. Nothing narrower will
    do: nanoseconds since the epoch needs 61 bits today, and a double would quantise
    them to 256 ns.
    """
    out = [f'{c.tdc}:aL:[0]' for c in choppers]
    out.append(f'{pulse_pv}:d:0.0')
    return out
