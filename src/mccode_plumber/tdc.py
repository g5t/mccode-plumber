"""Publish chopper top-dead-centre times while a simulation is running.

An ESS `NXdisk_chopper` wants a `tdct` stream: a vector of absolute nanoseconds saying
when the disc's mark passed its pickup. A simulation never measures that. It records the
two numbers the crossings follow from -- the disc's speed and its delay -- and the times
are `delay + k/|speed|` after a pulse.

`conductor.py` already computes them, for the replay that does not exist yet: there the
pulse instant comes from `readout-replay`, which knows exactly when it sent the events a
crossing has to be comparable with. The all-at-once flow has no replayer. `ReadoutCAEN`
emits its packets during the raytrace, at wall-clock time, so anything that must share
those timestamps has to be produced live -- which is all this server adds. The arithmetic
is `conductor.Chopper.crossings`, unchanged; only the clock is new, and only the clock
goes away when the replayer arrives.

It serves its own PVs rather than putting into the mailbox, for two reasons. The cadence
belongs here -- a vector per pulse, fourteen times a second, is not something a client
called once per scan point can drive -- and the names must be *exactly* the stream sources
the NeXus structure names, while the mailbox prefixes every name it serves.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from p4p.nt import NTScalar

from .conductor import NS, Chopper

#: The ESS source frequency. `Chopper.crossings` assumes it too, when deciding how many
#: turns of a disc fall inside one pulse.
PULSE_RATE = 14.0

#: What to publish as the per-pulse reference sample. A simulation has no accelerator, so
#: the value is a stand-in; its *timestamp* is the part everything else is measured from.
PULSE_VALUE = 1.0


def _scalar(value):
    """The number out of whatever a put handed us.

    A client put arrives as a whole NTScalar structure; a value set in-process is already
    a number. Both reach here, so both are read the same way.
    """
    try:
        value = value['value']
    except (TypeError, KeyError, IndexError):
        pass
    return float(value)


class InputHandler:
    """A mailbox PV that also remembers, so the pulse loop can read it.

    Deliberately not `epics.MailboxHandler`: that one only re-posts, and a value nothing
    keeps is a value the crossings cannot be computed from.
    """

    def __init__(self, name: str, values: dict):
        self.name = name
        self.values = values

    def put(self, pv, op):
        from datetime import datetime, timezone
        value = op.value()
        try:
            self.values[self.name] = _scalar(value)
        except (TypeError, ValueError):
            pass
        pv.post(value, timestamp=datetime.now(timezone.utc).timestamp())
        op.done()


@dataclass
class FakeTDC:
    """The choppers, their values, and whether to be publishing them.

    Held together by one rule, the same one `conductor.ChopperPublisher` keeps: every
    timestamp written comes from the pulse instant, never from a second reading of the
    clock. The two differ only in where the pulse comes from -- a grid tick here, the
    replayer there.
    """
    choppers: tuple[Chopper, ...] = ()
    pulse_pv: str = 'pulse'
    run_pv: str = 'tdc_run'
    rate: float = PULSE_RATE

    values: dict = field(default_factory=dict, init=False)
    pvs: dict = field(default_factory=dict, init=False)
    running: bool = field(default=False, init=False)

    # -- the PVs ---------------------------------------------------------------------

    def input_names(self) -> list[str]:
        """The parameter PVs a scan writes: speed, delay and, if it has one, park.

        Ordered and de-duplicated rather than a set, so the printed startup line reads the
        same way twice running.
        """
        names = []
        for chopper in self.choppers:
            for name in (chopper.speed, chopper.delay, chopper.park):
                if name and name not in names:
                    names.append(name)
        return names

    def strings(self) -> list[str]:
        """Every PV this server offers, in `mp-epics-strings` form.

        A TDC channel is `aL`, an array of unsigned 64-bit integers: nanoseconds since the
        epoch needs 61 bits today, and a double would quantise them to 256 ns. It starts
        *empty* rather than holding a zero -- a forwarder connecting before the run would
        otherwise record one crossing at the epoch.
        """
        out = [f'{c.tdc}:aL:[]' for c in self.choppers]
        out += [f'{name}:d:0.0' for name in self.input_names()]
        out.append(f'{self.pulse_pv}:d:0.0')
        out.append(f'{self.run_pv}:i:0')
        return out

    def provider(self):
        from p4p.server import StaticProvider
        from p4p.server.thread import SharedPV
        from .epics import strings_to_instr_par_nt

        provider = StaticProvider('tdc')
        outputs = {c.tdc for c in self.choppers} | {self.pulse_pv}
        for name, code, default in strings_to_instr_par_nt(self.strings()):
            if name in outputs:
                handler = None  # written by the pulse loop, not by anyone else
            elif name == self.run_pv:
                handler = RunHandler(self)
            else:
                handler = InputHandler(name, self.values)
                self.values[name] = float(default)
            pv = SharedPV(nt=NTScalar(code), initial=default, handler=handler)
            provider.add(name, pv)
            self.pvs[name] = pv
        return provider

    # -- publishing ------------------------------------------------------------------

    def emit(self, pulse_ns: int) -> None:
        """One pulse: the reference sample, then whatever crossed since it.

        Both are stamped with the pulse instant. Stamping with a fresh `time.time_ns()`
        would put the reference sample after the crossings measured from it.
        """
        import numpy as np
        timestamp = pulse_ns / NS
        self.pvs[self.pulse_pv].post(PULSE_VALUE, timestamp=timestamp)
        for chopper in self.choppers:
            times = chopper.crossings(pulse_ns, self.values)
            if times:
                self.pvs[chopper.tdc].post(
                    np.asarray(times, dtype=np.uint64), timestamp=timestamp)

    def run(self, ticks: int | None = None, clock=None, wait=None) -> None:
        """Tick the pulse grid, publishing on the ticks a run is in progress for.

        The grid is anchored on the epoch rather than on start-up, and the *tick* is taken
        as the pulse instant rather than the moment of waking, so the times published are
        exactly spaced whatever the sleep actually did. This is what
        `Sender::begin_pulse` does in mcstas-readout-master, so replayed events and these
        crossings land on the same grid.

        The grid runs whether or not a run does; only publishing is gated. Skipping ticks
        while stopped and resuming would shift the phase, and the gap in the log is the
        honest record of a period with nothing running.

        ``ticks`` bounds the loop for tests; the server runs unbounded.
        """
        from time import sleep, time_ns
        clock = clock or time_ns
        wait = wait or sleep
        period_ns = int(round(NS / self.rate))
        count = 0
        while ticks is None or count < ticks:
            now = clock()
            pulse_ns = (now // period_ns + 1) * period_ns
            wait((pulse_ns - now) / NS)
            if self.running:
                self.emit(pulse_ns)
            count += 1


class RunHandler:
    """The one control: non-zero starts publishing, zero stops it.

    A PV rather than a socket because the process that knows when a run starts is
    `mp-nexus-splitrun`, which is started separately from this server and shares no state
    with it. They already have PVA between them.
    """

    def __init__(self, faker: FakeTDC):
        self.faker = faker

    def put(self, pv, op):
        from datetime import datetime, timezone
        value = op.value()
        was = self.faker.running
        self.faker.running = bool(_scalar(value))
        if self.faker.running != was:
            print(f'info: top-dead-centre publishing '
                  f'{"started" if self.faker.running else "stopped"}', flush=True)
        pv.post(value, timestamp=datetime.now(timezone.utc).timestamp())
        op.done()


def main(choppers, pulse_pv: str = 'pulse', run_pv: str = 'tdc_run',
         rate: float = PULSE_RATE) -> None:
    from p4p.server import Server
    faker = FakeTDC(tuple(choppers), pulse_pv=pulse_pv, run_pv=run_pv, rate=rate)
    provider = faker.provider()
    with Server(providers=[provider]):
        print(f'Starting fake top-dead-centre server for {len(faker.choppers)} '
              f'chopper(s) at {rate} Hz, waiting on {run_pv}', flush=True)
        faker.run()


def parse_chopper(text: str) -> Chopper:
    """`name,tdc,speed,delay[,park]` -- the five names a crossing needs.

    Two namespaces in one argument, on purpose: `tdc` is a control-system channel while
    `speed` and `delay` are McStas parameter names. They are separate fields in `Chopper`
    for the same reason.
    """
    parts = [p.strip() for p in text.split(',')]
    if len(parts) not in (4, 5):
        raise ValueError(
            f'Expected name,tdc,speed,delay[,park] for a chopper; got {text!r}')
    return Chopper(*parts)


def get_parser():
    from argparse import ArgumentParser
    from mccode_plumber import __version__
    p = ArgumentParser(
        description='Serve fake chopper top-dead-centre times on the pulse grid')
    p.add_argument('-c', '--chopper', type=parse_chopper, action='append', default=[],
                   metavar='name,tdc,speed,delay[,park]',
                   help='One disc chopper; repeat for each')
    p.add_argument('--pulse-pv', type=str, default='pulse',
                   help='PV carrying one reference sample per pulse')
    p.add_argument('--run-pv', type=str, default='tdc_run',
                   help='PV that starts (non-zero) and stops (zero) publishing')
    p.add_argument('--rate', type=float, default=PULSE_RATE,
                   help='Source frequency in Hz')
    p.add_argument('-v', '--version', action='version', version=__version__)
    return p


def run():
    args = get_parser().parse_args()
    main(args.chopper, pulse_pv=args.pulse_pv, run_pv=args.run_pv, rate=args.rate)


if __name__ == '__main__':
    run()
