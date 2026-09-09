from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from mccode_plumber.conductor import Chopper
from mccode_plumber.tdc import PULSE_RATE
from .manager import Manager
from .ensure import ensure_executable


@dataclass
class TDCFaker(Manager):
    """Command and control of the fake top-dead-centre server for an instrument.

    A service rather than part of a run: the PVs have to exist before the forwarder is
    configured and stay there between scans, while `mp-nexus-splitrun` comes and goes.
    Which run is in progress is said over `run_pv` instead.

    Parameters
    ----------
    choppers:  the discs to publish crossings for, with the PV and parameter names taken
               from the NeXus structure the file-writer will be filling
    pulse_pv:  PV carrying one reference sample per pulse
    run_pv:    PV that starts (non-zero) and stops (zero) publishing
    rate:      source frequency in Hz
    """
    choppers: tuple[Chopper, ...]
    pulse_pv: str = 'pulse'
    run_pv: str = 'tdc_run'
    rate: float = PULSE_RATE
    _command: Path = field(default_factory=lambda: Path('mp-tdc'))

    def __post_init__(self):
        self._command = ensure_executable(self._command)

    def __run_command__(self) -> list[str]:
        argv = [self._command.as_posix(), '--pulse-pv', self.pulse_pv,
                '--run-pv', self.run_pv, '--rate', str(self.rate)]
        for c in self.choppers:
            names = [c.name, c.tdc, c.speed, c.delay] + ([c.park] if c.park else [])
            argv += ['--chopper', ','.join(names)]
        return argv
