from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from multiprocessing import Process


def ensure_executable(path: Path):
    from shutil import which
    found = which(path)
    if found is None:
        raise FileNotFoundError(path)
    return Path(found)

def ensure_readable_file(path: Path):
    from os import access, R_OK
    if isinstance(path, str):
        path = Path(path)
    if not isinstance(path, Path):
        raise ValueError(f'{path} is not a Path object')
    if not path.exists():
        raise ValueError(f'{path} does not exist')
    if not path.is_file():
        raise ValueError(f'{path} is not a file')
    if not access(path, R_OK):
        raise ValueError(f'{path} is not readable')
    return path

def ensure_writable_directory(path: Path):
    from os import access, W_OK
    if isinstance(path, str):
        path = Path(path)
    if not isinstance(path, Path):
        raise ValueError(f'{path} is not a Path object')
    if not path.exists():
        raise ValueError(f'{path} does not exist')
    if not path.is_dir():
        raise ValueError(f'{path} is not a directory')
    if not access(path, W_OK):
        raise ValueError(f'{path} is not writable')
    return path


def ensure_path(path: Path, access_type, is_dir: bool = False) -> Path:
    from os import access
    if isinstance(path, str):
        path = Path(path)
    if not isinstance(path, Path):
        raise ValueError(f'{path} is not a Path object')
    if not path.exists():
        raise ValueError(f'{path} does not exist')
    if is_dir and not path.is_dir():
        raise ValueError(f'{path} is not a directory')
    if not is_dir and not path.is_file():
        raise ValueError(f'{path} is not a file')
    if not access(path, access_type):
        raise ValueError(f'{path} does not support {access_type}')
    return path


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
