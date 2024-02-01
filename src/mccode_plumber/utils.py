from pathlib import Path


def is_accessible(access_type):
    def checker(name: str | None | Path):
        if name is None or name == '':
            return None
        from os import access
        if not isinstance(name, Path):
            name = Path(name).resolve()
        if not name.exists():
            raise RuntimeError(f'The specified filename {name} does not exist')
        if not access(name, access_type):
            raise RuntimeError(f'The specified filename {name} is not {access_type}')
        return name

    return checker


def is_readable(value: str | None | Path):
    from os import R_OK as READABLE
    return is_accessible(READABLE)(value)


def is_writable(value: str | None | Path):
    from os import W_OK
    return is_accessible(W_OK)(value)


def is_appendable(value: str | None | Path):
    from os import W_OK, R_OK
    return is_accessible(R_OK | W_OK)(value)


def is_executable(value: str | None | Path):
    from os import X_OK as EXECUTABLE
    return is_accessible(EXECUTABLE)(value)


def is_callable(name: str | None):
    if name is None:
        return None
    from importlib import import_module
    module_name, func_name = name.split(':')
    module = import_module(module_name)
    return getattr(module, func_name)