from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from multiprocessing import Process


@dataclass
class Manager:
    """
    Command and control of a process

    Properties
    ----------
    _process:   a multiprocessing.Process instance, which is undefined for a short
                period during instance creation inside the `start` class method
    """
    _process: Process | None

    def __run_command__(self) -> list[str]:
        pass

    def run(self, capture=True):
        from subprocess import run, STDOUT
        root = Path(__file__).parent
        argv = self.__run_command__()
        if isinstance(capture, bool):
            return run(argv, capture_output=capture, cwd=root, text=True)
        else:
            return run(argv, cwd=root, stdout=capture, stderr=STDOUT)

    def finalize(self):
        pass

    @classmethod
    def fieldnames(cls) -> list[str]:
        from dataclasses import fields
        return [field.name for field in fields(cls)]

    @classmethod
    def start(cls, capture=True, **config):
        names = cls.fieldnames()
        kwargs = {k: config[k] for k in names if k in config}
        if any(k not in names for k in config):
            raise ValueError(f'{config} expected to contain only {names}')
        if '_process' not in kwargs:
            kwargs['_process'] = None
        manager = cls(**kwargs)
        manager._process = Process(target=manager.run, args=(capture,))
        manager._process.start()
        return manager

    def stop(self):
        self.finalize()
        self._process.terminate()
