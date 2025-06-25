from __future__ import annotations
from dataclasses import dataclass, field
from mccode_antlr.common import InstrumentParameter
from p4p.nt import NTScalar
from mccode_plumber.manage.manager import Manager

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
    values: dict[str, NTScalar] = field(default_factory=dict)

    def __post_init__(self):
        from mccode_plumber.epics import convert_instr_parameters_to_nt
        if not len(self.values):
            self.values = convert_instr_parameters_to_nt(self.parameters)

    @classmethod
    def start(cls, capture: bool = True, **config):
        from multiprocessing import Process
        from mccode_plumber.epics import main
        names = cls.fieldnames()
        kwargs = {k: config[k] for k in names if k in config}
        obj = cls(**kwargs, _process=None)
        obj._process = Process(target=main, args=(obj.values, obj.prefix))
        obj._process.start()
        return obj

    def stop(self):
        self._process.terminate()
        self._process.join(1)
        self._process.close()
