from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from multiprocessing import Process, Pipe
from multiprocessing.connection import Connection
from enum import Enum
from colorama import Fore, Back, Style

class IOType(Enum):
    stdout = 1
    stderr = 2


@dataclass
class Manager:
    """
    Command and control of a process

    Properties
    ----------
    _process:   a multiprocessing.Process instance, which is undefined for a short
                period during instance creation inside the `start` class method
    """
    name: str
    style: Style
    _process: Process | None
    _connection: Connection | None

    def __run_command__(self) -> list[str]:
        pass

    def finalize(self):
        pass

    @classmethod
    def fieldnames(cls) -> list[str]:
        from dataclasses import fields
        return [field.name for field in fields(cls)]

    @classmethod
    def start(cls, **config):
        names = cls.fieldnames()
        kwargs = {k: config[k] for k in names if k in config}
        if any(k not in names for k in config):
            raise ValueError(f'{config} expected to contain only {names}')
        if '_process' not in kwargs:
            kwargs['_process'] = None
        if '_connection' not in kwargs:
            kwargs['_connection'] = None
        if 'name' not in kwargs:
            kwargs['name'] = 'Managed process'
        if 'style' not in kwargs:
            kwargs['style'] = Fore.WHITE + Back.BLACK
        manager = cls(**kwargs)
        manager._connection, child_conn = Pipe()
        manager._process = Process(target=manager.run, args=(child_conn,))
        manager._process.start()
        return manager

    def stop(self):
        self.finalize()
        self._process.terminate()

    def poll(self):
        from sys import stderr
        attn = Fore.BLACK + Back.RED + Style.BRIGHT
        # check for anything received on our end of the connection
        while self._connection.poll():
            # examine what was returned:
            try:
                ret = self._connection.recv()
            except EOFError:
                print(f'{attn}{self.name}: [unexpected halt]{Style.RESET_ALL}')
                return False
            if len(ret) == 2:
                t, line = ret
                line = f'{self.style}{self.name}:{Style.RESET_ALL} {line}'
                if t == IOType.stdout:
                    print(line, end='')
                else:
                    print(line, file=stderr, end='')
            else:
                print(f'{attn}{self.name}: [unknown received data on connection]{Style.RESET_ALL}')
        return self._process.is_alive()

    def run(self, conn):
        from subprocess import Popen, PIPE
        from select import select
        argv = self.__run_command__()

        shell = isinstance(argv, str)
        conn.send((IOType.stdout, f'Starting {argv if shell else " ".join(argv)}\n'))
        process = Popen(argv, shell=shell, stdout=PIPE, stderr=PIPE, bufsize=1, universal_newlines=True, )
        out, err = process.stdout.fileno(), process.stderr.fileno()
        check = [process.stdout, process.stderr]
        while process.poll() is None:
            r, w, x = select(check, [], check, 0.5,)
            for stream in r:
                if stream.fileno() == out:
                    conn.send((IOType.stdout, process.stdout.readline()))
                elif stream.fileno() == err:
                    conn.send((IOType.stderr, process.stderr.readline()))
            for stream in x:
                if stream.fileno() == out:
                    conn.send((IOType.stdout, "EXCEPTION ON STDOUT"))
                elif stream.fileno() == err:
                    conn.send((IOType.stderr, "EXCEPTION ON STDERR"))
        # Process finished, but the buffers may still contain data:
        for stream in check:
            if stream.fileno() == out:
                map(lambda line: conn.send(IOType.stdout, line), stream.readlines())
            elif stream.fileno() == err:
                map(lambda line: conn.send(IOType.stderr, line), stream.readlines())