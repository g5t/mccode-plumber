from __future__ import annotations
from pathlib import Path
from os import access, R_OK, W_OK, X_OK

def message(mode) -> str:
    return {R_OK: 'readable', W_OK: 'writable', X_OK: 'executable'}.get(mode, 'unknown')

def ensure_executable(path: str| Path) -> Path:
    from shutil import which
    found = which(path)
    if found is None:
        raise FileNotFoundError(path)
    return Path(found)

def ensure_accessible_file(path: str| Path, mode, must_exist=True) -> Path:
    if isinstance(path, str):
        path = Path(path)
    if not isinstance(path, Path):
        raise ValueError(f'{path} is not a Path object')
    if must_exist:
        if not path.exists():
            raise ValueError(f'{path} does not exist')
        if not path.is_file():
            raise ValueError(f'{path} is not a file')
        if not access(path, mode):
            raise ValueError(f'{path} is not {message(mode)}')
    return path

def ensure_accessible_directory(path: str| Path, mode) -> Path:
    if isinstance(path, str):
        path = Path(path)
    if not isinstance(path, Path):
        raise ValueError(f'{path} is not a Path object')
    if not path.exists():
        raise ValueError(f'{path} does not exist')
    if not path.is_dir():
        raise ValueError(f'{path} is not a directory')
    if not access(path, mode):
        raise ValueError(f'{path} is not a {message(mode)} directory')
    return path

def ensure_readable_file(path: str| Path) -> Path:
    return ensure_accessible_file(path, R_OK)

def ensure_writable_file(path: str| Path) -> Path:
    if not isinstance(path, Path):
        path = Path(path)
    return (
            ensure_accessible_directory(path.parent, W_OK)
            and ensure_accessible_file(path, W_OK, must_exist=False)
    )

def ensure_readable_directory(path: str| Path) -> Path:
    return ensure_accessible_directory(path, R_OK)

def ensure_writable_directory(path: str| Path) -> Path:
    return ensure_accessible_directory(path, W_OK)


def ensure_path(path: str| Path, access_type, is_dir: bool = False) -> Path:
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
