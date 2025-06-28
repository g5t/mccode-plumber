from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from mccode_antlr.common import InstrumentParameter
from .manager import Manager
from .ensure import ensure_executable

@dataclass
class EPICSMailbox(Manager):
    """
    Command and control of an EPICS Mailbox server for an instrument

    Parameters
    ----------
    parameters: the instrument parameters which define the PV values
    prefix:     a PV value prefix to use with all instrument-defined parameters
    strings:    optional list of NT parameter information to configure the
                mailbox when the instrument parameters are not available for
                use in determining the same information.
    """
    parameters: tuple[InstrumentParameter, ...]
    prefix: str
    strings: list[str] = field(default_factory=list)
    _command: Path = field(default_factory=lambda: Path('mp-epics-strings'))

    def __post_init__(self):
        from mccode_plumber.epics import instr_par_nt_to_strings
        self._command = ensure_executable(self._command)
        if not len(self.strings):
            self.strings = instr_par_nt_to_strings(self.parameters)

    def __run_command__(self) -> list[str]:
        return [self._command.as_posix(), '--prefix', self.prefix] + self.strings
