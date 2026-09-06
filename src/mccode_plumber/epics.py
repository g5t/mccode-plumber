#!/usr/bin/env python3
from __future__ import annotations

from p4p.nt import NTScalar
from p4p.server import Server, StaticProvider
from p4p.server.thread import SharedPV
from pathlib import Path
from typing import Union

def instr_par_to_nt_primitive(parameters):
    from mccode_antlr.common.expression import DataType, ShapeType
    out = []
    for p in parameters:
        expr = p.value
        if expr.is_str:
            t, d = 's', ''
        elif expr.data_type == DataType.int:
            t, d = 'i', 0
        elif expr.data_type == DataType.float:
            t, d = 'd', 0.0
        else:
            raise ValueError(f"Unknown parameter type {expr.data_type}")
        if expr.shape_type == ShapeType.vector:
            t, d = 'a' + t, [d]
        out.append((p.name, t, d))
    return out

def instr_par_nt_to_strings(parameters):
    return [f'{n}:{t}:{d}'.replace(' ','') for n, t, d in instr_par_to_nt_primitive(parameters)]

#: pvData scalar type codes, by the Python type that parses one from a string.
#: Signed and unsigned integers of every width are `int`; both float widths are `float`.
#: The wide unsigned codes matter here: a top-dead-centre timestamp is nanoseconds since
#: the epoch, which needs 61 bits today, so it fits `L` (uint64) and nothing narrower.
_TYPE_CODE_PARSERS = (('s', str), ('bBhHiIlL', int), ('fd', float))


def nt_type_parser(code: str):
    """The Python type that reads one element of a pvData type code from a string."""
    element = code[1:] if code.startswith('a') else code
    for codes, parser in _TYPE_CODE_PARSERS:
        if element in codes:
            return parser
    raise ValueError(
        f"Unknown pvData type code {code!r}; expected one of "
        f"{''.join(codes for codes, _ in _TYPE_CODE_PARSERS)}, optionally prefixed 'a'")


def strings_to_instr_par_nt(strings):
    out = []
    for string in strings:
        # From the right: a real ESS PV name is itself colon-separated
        # (`BIFRO-ChpSy1:Chop-PSC-101:00-TS-I`), so only the last two colons delimit the
        # type and the default. Splitting from the left works for bare McStas parameter
        # names and for nothing else.
        name, t, dstr = string.rsplit(':', 2)
        # `nt_type_parser` raises on an unknown code. This used to *construct* a
        # ValueError and drop it, leaving `trans` as None to fail obscurely further on.
        trans = nt_type_parser(t)
        if t.startswith('a'):
            d = [trans(x) for x in dstr.translate(str.maketrans(',',' ','[]')).split()]
        else:
            d = trans(dstr)
        out.append((name, t, d))
    return out

def convert_strings_to_nt(strings):
    return {n: NTScalar(t).wrap(d) for n, t, d in strings_to_instr_par_nt(strings)}

def convert_instr_parameters_to_nt(parameters):
    out = {n: NTScalar(t).wrap(d) for n, t, d in instr_par_to_nt_primitive(parameters)}
    return out


def parse_instr_nt_values(instr: Union[Path, str]):
    """Get the instrument parameters from an Instr a or a parseable Instr file and convert to NTScalar values"""
    from .mccode import get_mccode_instr_parameters
    nts = convert_instr_parameters_to_nt(get_mccode_instr_parameters(instr))
    if 'mcpl_filename' not in nts:
        nts['mcpl_filename'] = NTScalar('s').wrap('')
    return nts


class MailboxHandler:
    @staticmethod
    def put(pv, op):
        from datetime import datetime, timezone
        val = op.value()

        if pv.nt is None:
            # Assume that this means wrap wasn't provided ...
            pv.nt = NTScalar(val.type()['value'])
            pv._wrap = pv.nt.wrap

        # Notify any subscribers of the new value, adding the timestamp, so they know when it was set.
        pv.post(val, timestamp=datetime.now(timezone.utc).timestamp())
        # Notify the client making this PUT operation that it has now completed
        op.done()


def get_parser():
    from argparse import ArgumentParser
    from mccode_plumber import __version__
    p = ArgumentParser()
    p.add_argument('instr', type=str, help='The instrument file to read')
    p.add_argument('-p', '--prefix', type=str, help='The EPICS PV prefix to use', default='mcstas:')
    p.add_argument('-v', '--version', action='version', version=__version__)
    return p


def parse_args():
    args = get_parser().parse_args()
    parameters = parse_instr_nt_values(args.instr)
    return parameters, args


def main(names: dict[str, NTScalar], prefix: str | None = None, filename_required: bool = True):
    provider = StaticProvider('mailbox')  # 'mailbox' is an arbitrary name

    if filename_required and 'mcpl_filename' not in names:
        names['mcpl_filename'] = NTScalar('s').wrap('')

    pvs = []  # we must keep a reference in order to keep the Handler from being collected
    for name, value in names.items():
        pv = SharedPV(initial=value, handler=MailboxHandler())
        provider.add(f'{prefix}{name}' if prefix else name, pv)
        pvs.append(pv)

    print(f'Start mailbox server for {len(pvs)} PVs with prefix {prefix}')
    Server.forever(providers=[provider])
    print('Done')


def run():
    parameters, args = parse_args()
    main(parameters, prefix=args.prefix)


def start(parameters, prefix: str | None = None):
    from multiprocessing import Process
    proc = Process(target=main, args=(parameters, prefix))
    proc.start()
    return proc


def stop(proc):
    proc.terminate()
    proc.join(1)
    proc.close()


def parse_like(current, text: str):
    """Read ``text`` as whatever the PV currently holds.

    A PV knows its own type, so the value it already has says how to read the string --
    which is how a scalar has always been updated here. Arrays are read the same way,
    element by element: a chopper's top-dead-centre PV holds a vector of nanosecond
    timestamps, and the string ``[1,2,3]`` has to reach it as three integers rather than
    as one unparseable scalar.
    """
    import numpy as np
    if isinstance(current, str):
        return text
    if isinstance(current, (np.ndarray, list, tuple)):
        dtype = getattr(np.asarray(current), 'dtype', None)
        element = int if dtype is not None and dtype.kind in 'iub' else float
        items = text.translate(str.maketrans(',', ' ', '[]')).split()
        return np.asarray([element(x) for x in items],
                          dtype=dtype if dtype is not None else None)
    if isinstance(current, bool):
        return bool(int(text))
    if isinstance(current, int):
        return int(text)
    if isinstance(current, float):
        return float(text)
    raise ValueError(f'unknown type {type(current)}')


def update():
    from argparse import ArgumentParser
    from p4p.client.thread import Context
    parser = ArgumentParser(description="Update the mailbox server with new values")
    parser.add_argument('address value', type=str, nargs='+', help='The mailbox address and value to be updated')
    args = parser.parse_args()
    addresses_values = getattr(args, 'address value')

    if len(addresses_values) == 0:
        parser.print_help()
        return

    addresses = addresses_values[::2]
    values = addresses_values[1::2]

    if len(addresses_values) % 2:
        print(f'Please provide address-value pairs. Provided {addresses=} {values=}')

    ctx = Context('pva')
    for address, value in zip(addresses, values):
        pv = ctx.get(address, throw=False)
        if isinstance(pv, TimeoutError):
            print(f'[Timeout] Failed to update {address} with {value} (Unknown to EPICS?)')
            continue
        try:
            ctx.put(address, parse_like(pv, value))
        except ValueError as error:
            raise ValueError(f'Address {address}: {error}') from None

    ctx.disconnect()


def get_strings_parser():
    from argparse import ArgumentParser
    from mccode_plumber import __version__
    p = ArgumentParser()
    p.add_argument('strings', type=str, nargs='+', help='The string encoded NTScalars to read, each name:type-char:default')
    p.add_argument('-p', '--prefix', type=str, help='The EPICS PV prefix to use', default='mcstas:')
    p.add_argument('-v', '--version', action='version', version=__version__)
    return p


def run_strings():
    args = get_strings_parser().parse_args()
    main(convert_strings_to_nt(args.strings), prefix=args.prefix)



if __name__ == '__main__':
    run()
