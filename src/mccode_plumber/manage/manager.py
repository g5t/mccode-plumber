from __future__ import annotations
from dataclasses import dataclass
from subprocess import Popen, PIPE
from threading import Thread
from enum import Enum
from colorama import Fore, Back, Style
from colorama.ansi import AnsiStyle


class IOType(Enum):
    stdout = 1
    stderr = 2


@dataclass
class Manager:
    """
    Command and control of a process

    Properties
    ----------
    _process:   a subprocess.Popen instance
    """
    name: str
    style: AnsiStyle
    _process: Popen | None
    _stdout_thread: Thread | None
    _stderr_thread: Thread | None

    def __run_command__(self) -> list[str]:
        return []

    def finalize(self):
        pass

    @classmethod
    def fieldnames(cls) -> list[str]:
        from dataclasses import fields
        return [field.name for field in fields(cls)]

    def _read_stream(self, stream, io_type: IOType):
        """Read lines from stream and print them until EOF.

        This replaces the previous behaviour of sending lines over a
        multiprocessing Connection. Printing directly from the reader
        threads is sufficient because the manager previously only used
        the connection to relay subprocess stdout/stderr back to the
        parent process for display.
        """
        try:
            for line in iter(stream.readline, ''):
                if not line:
                    break
                # format and print the line, preserving original behaviour
                formatted = f'{self.style}{self.name}:{Style.RESET_ALL} {line}'
                if io_type == IOType.stdout:
                    print(formatted, end='')
                else:
                    from sys import stderr
                    print(formatted, file=stderr, end='')
        except ValueError:
            pass  # stream closed
        finally:
            try:
                stream.close()
            except Exception:
                pass

    @classmethod
    def start(cls, **config):
        names = cls.fieldnames()
        kwargs = {k: config[k] for k in names if k in config}
        if any(k not in names for k in config):
            raise ValueError(f'{config} expected to contain only {names}')
        for p in ('_process', '_stdout_thread', '_stderr_thread'):
            if p not in kwargs:
                kwargs[p] = None
        if 'name' not in kwargs:
            kwargs['name'] = 'Managed process'
        if 'style' not in kwargs:
            kwargs['style'] = Fore.WHITE + Back.BLACK

        manager = cls(**kwargs)

        argv = manager.__run_command__()
        shell = isinstance(argv, str)
        # announce start directly instead of sending via a Connection
        print(f'Starting {argv if shell else " ".join(argv)}')

        manager._process = Popen(
            argv, shell=shell, stdout=PIPE, stderr=PIPE, bufsize=1,
            universal_newlines=True,
        )
        manager._stdout_thread = Thread(
            target=manager._read_stream,
            args=(manager._process.stdout, IOType.stdout),
            daemon=True,
        )
        manager._stderr_thread = Thread(
            target=manager._read_stream,
            args=(manager._process.stderr, IOType.stderr),
            daemon=True,
        )
        manager._stdout_thread.start()
        manager._stderr_thread.start()
        return manager

    def stop(self):
        self.finalize()
        if self._process:
            self._process.terminate()
            self._process.wait()

    def poll(self):
        """Check whether the managed process is still running.

        Previously this drained and printed any messages received over a
        multiprocessing Connection. Reader threads now handle printing,
        so poll only needs to report process liveness.
        """
        if not self._process:
            return False
        return self._process.poll() is None
