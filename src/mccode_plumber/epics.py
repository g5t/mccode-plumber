#!/usr/bin/env python3
from p4p.nt import NTScalar
from p4p.server import Server, StaticProvider
from p4p.server.thread import SharedPV
from pathlib import Path
from typing import Union


def convert_instr_parameters_to_nt(parameters):
    from mccode_antlr.common.expression import DataType, ShapeType
    out = {}
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
        out[p.name] = NTScalar(t).wrap(expr.value if expr.has_value else d)
    return out


def parse_instr_nt_values(instr: Union[Path, str]):
    """Get the instrument parameters from a pickled Instr a or a parseable Instr file and convert to NTScalar values"""
    from .mccode import get_mccode_instr_parameters
    return convert_instr_parameters_to_nt(get_mccode_instr_parameters(instr))


class MailboxHandler:
    @staticmethod
    def put(pv, op):
        val = op.value()
        # logging.info("Assign %s = %s", op.name(), val)
        # Notify any subscribers of the new value.
        # Also set timeStamp with current system time.
        # FIXME timestamp can not be wrapped because the pv tries to use an empty lambda?!
        pv.post(val)  # pv.post(val, timestamp=time.time())
        # Notify the client making this PUT operation that it has now completed
        op.done()


def get_parser():
    from argparse import ArgumentParser
    p = ArgumentParser()
    p.add_argument('instr', type=str, help='The instrument file to read')
    p.add_argument('-p', '--prefix', type=str, help='The EPICS PV prefix to use', default='mcstas:')
    return p


def parse_args():
    args = get_parser().parse_args()
    parameters = parse_instr_nt_values(args.instr)
    return parameters, args


def main(names: dict[str, NTScalar], prefix: str = None):
    provider = StaticProvider('mailbox')  # 'mailbox' is an arbitrary name

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


if __name__ == '__main__':
    run()
