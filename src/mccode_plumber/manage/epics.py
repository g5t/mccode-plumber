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
    values:     optional dictionary of PV name: value pairs, if the instrument
                is not to be used for determining which PVs should be used
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

    # @classmethod
    # def start(cls, capture: bool = True, **config):
    #     from multiprocessing import Process
    #     from mccode_plumber.epics import main
    #     names = cls.fieldnames()
    #     kwargs = {k: config[k] for k in names if k in config}
    #     obj = cls(**kwargs, _process=None)
    #     obj._process = Process(target=main, args=(obj.values, obj.prefix))
    #     obj._process.start()
    #     return obj
    #
    # def stop(self):
    #     self._process.terminate()
    #     self._process.join(1)
    #     self._process.close()
