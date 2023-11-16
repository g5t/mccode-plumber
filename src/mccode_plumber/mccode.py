from pathlib import Path
from typing import Union
from mccode_antlr.instr import Instr
from mccode_antlr.common import InstrumentParameter
import pickle


def get_mcstas_instr(filename: Union[Path, str]) -> Instr:
    from restage.instr import load_instr
    return load_instr(filename)

def get_mccode_instr_parameters(filename: Union[Path, str]) -> tuple[InstrumentParameter]:
    from mccode_antlr.loader.loader import parse_mccode_instr_parameters
    if not isinstance(filename, Path):
        filename = Path(filename)
    if filename.suffix == '.instr':
        with filename.open('r') as file:
            contents = file.read()
        return parse_mccode_instr_parameters(contents)
    # otherwise:
    return get_mcstas_instr(filename).parameters


def pickle_mccode_instr(instr: Instr, filename: Union[Path, str]):
    """Save a mccode_antlr.instr.Instr to a pickle file

    Note:
        This is intended to be used along with the plumber(s) in multiple containers to avoid
        having to reparse the instrument file for each container. While also avoiding the
        possibility of pickle-incompatibility between different versions of mccode_antlr.
    """
    if not isinstance(filename, Path):
        filename = Path(filename)
    with filename.open('wb') as file:
        pickle.dump(instr, file)
