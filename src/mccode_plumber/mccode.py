from pathlib import Path
from typing import Union


def get_mcstas_instr(filename: Union[Path, str]):
    from restage.instr import load_instr
    return load_instr(filename)


def get_mccode_instr_parameters(filename: Union[Path, str]):
    from mccode_antlr.loader.loader import parse_mccode_instr_parameters
    if not isinstance(filename, Path):
        filename = Path(filename)
    if filename.suffix == '.instr':
        with filename.open('r') as file:
            contents = file.read()
        return parse_mccode_instr_parameters(contents)
    # otherwise:
    return get_mcstas_instr(filename).parameters


def insert_mcstas_hdf5(filename: Union[Path, str], outfile: Union[Path, str], parent: str):
    import h5py
    from mccode_antlr.io.hdf5 import HDF5IO
    if isinstance(filename, str):
        filename = Path(filename)
    with h5py.File(outfile, mode='r+') as dest_file:
        if parent in dest_file:
            raise RuntimeError(f'{outfile} already contains an object named {parent}')
        dest = dest_file.create_group(parent)
        if filename.stem.lower() in ('.h5', '.hdf', '.hdf5', ):
            # copy the file contents if it _is_ a serialized instrument
            with h5py.File(filename, mode='r') as source:
                for obj in source.keys():
                    source.copy(obj, dest)
        else:
            instr = get_mcstas_instr(filename)
            HDF5IO.save(dest, instr)


def get_arg_parser():
    from argparse import ArgumentParser
    from mccode_plumber import __version__
    from .utils import is_readable, is_appendable
    parser = ArgumentParser(description="Copy a Instr HDF5 representation to a NeXus HDF5 file")
    a = parser.add_argument
    a('instrument', type=is_readable, default=None, help="The mcstas instrument file")
    a('-p', '--parent', type=str, default='mcstas')
    a('-o', '--outfile', type=is_appendable, default=None, help='Base NeXus structure, will be extended')
    a('-v', '--version', action='version', version=__version__)
    return parser


def insert():
    parser = get_arg_parser()
    args = parser.parse_args()
    insert_mcstas_hdf5(args.instrument, args.outfile, args.parent)


