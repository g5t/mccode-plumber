from __future__ import annotations
from dataclasses import dataclass, field
from subprocess import Popen, PIPE
from threading import Thread
from enum import Enum
from colorama import Fore, Back, Style
from colorama.ansi import AnsiStyle


class IOType(Enum):
    stdout = 1
    stderr = 2


@dataclass
class Triage:
    level: str = field(default=lambda: 'info')
    ignore: list[str] = field(default_factory=list)
    patterns: dict[str, list[str]] = field(default_factory=lambda: {
        'critical': [r'\bcritical\b', r'^cri'],
        'error': [r'\berror\b', r'exception', r'traceback', r'^err' ],
        'warning': [r'\bwarn(ing)?\b', r'deprecated', r'^war'],
        'notice': [r'\bnotice\b', r'^not'],
        'info': [r'\binfo\b', r'starting', r'done'],
        'hint': [r'\bhint\b'],
        'debug': [r'\bdebug\b', r'^deb', r'^dbg'],
    })
    styles: dict[str, str] = field(default_factory=lambda: {
        'critical': Fore.MAGENTA + Style.BRIGHT,
        'error': Fore.RED + Style.BRIGHT,
        'warning': Fore.YELLOW + Style.BRIGHT,
        'notice': Fore.CYAN + Style.BRIGHT,
        'info': Fore.GREEN,
        'hint': Fore.BLUE,
        'debug': Fore.WHITE + Style.BRIGHT,
        'default': Fore.RESET,
    })

    def _filtered_level(self, level: str) -> bool:
        def _level_value(v: str):
            for i, lvl in enumerate(self.patterns.keys()):
                if v == lvl:
                    return i
            return -1
        return _level_value(level) > _level_value(self.level)

    def _style_line(self, level: str, line: str):
        return self.styles.get(level, '') + line + Style.RESET_ALL

    def __call__(self, line: str) -> tuple[bool, str | None]:
        import re
        # If the line contains an ignored keyword, ignore it.
        if any(kw in line for kw in self.ignore):
            return True, None
        # Check if we can identify the status level of this message
        for level, patterns in self.patterns.items():
            for pattern in patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    return self._filtered_level(level), self._style_line(level, line)
        return self._filtered_level('default'), self._style_line('default', line)


@dataclass
class Manager:
    """
    Command and control of a process

    Properties
    ----------
    name: str
        The name of the process, used as a prefix for all printed status messages
    style: AnsiStyle
        Format string to style the printed process name
    _triage: Triage
        An object to filter status messages and identify severity levels
        applying its own message styling based on the identified level
    _process:   a subprocess.Popen instance
    _stdout_thread: Thread
    _stderr_thread: Thread
    """
    name: str
    style: AnsiStyle
    triage: Triage
    _process: Popen | None
    _stdout_thread: Thread | None
    _stderr_thread: Thread | None
    _name_padding: int

    def __run_command__(self) -> list[str]:
        return []

    def finalize(self):
        pass

    @classmethod
    def fieldnames(cls) -> list[str]:
        from dataclasses import fields
        return [field.name for field in fields(cls)]

    @property
    def name_padding(self):
        return self._name_padding

    @name_padding.setter
    def name_padding(self, value: int):
        self._name_padding = value

    def _pretty_name(self):
        padding = ' ' * self.name_padding
        return f'{self.style}{self.name}:{Style.RESET_ALL}{padding}'

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
                ignored, line = self.triage(line)
                if ignored:
                    continue
                formatted = f'{self._pretty_name()} {line}'
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
        if 'triage' not in kwargs:
            kwargs['triage'] = Triage()
        if '_name_padding' not in kwargs:
            kwargs['_name_padding'] = 0

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
