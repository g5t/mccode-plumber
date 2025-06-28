#!/usr/bin/env python3
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

def strings_to_instr_par_nt(strings):
    out = []
    for string in strings:
        name, t, dstr = string.split(':')
        trans = None
        if 'i' in t:
            trans = int
        elif 'd' in t:
            trans = float
        elif 's' in t:
            trans = str
        else:
            ValueError(f"Unknown type in {string}")
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


def main(names: dict[str, NTScalar], prefix: str = None, filename_required: bool = True):
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


def start(parameters, prefix: str = None):
    from multiprocessing import Process
    proc = Process(target=main, args=(parameters, prefix))
    proc.start()
    return proc


def stop(proc):
    proc.terminate()
    proc.join(1)
    proc.close()


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
        if isinstance(pv, float):
            ctx.put(address, float(value))
        elif isinstance(pv, int):
            ctx.put(address, int(value))
        elif isinstance(pv, str):
            ctx.put(address, str(value))
        elif isinstance(pv, TimeoutError):
            print(f'[Timeout] Failed to update {address} with {value} (Unknown to EPICS?)')
        else:
            raise ValueError(f'Address {address} has unknown type {type(pv)}')

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
