from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version('mccode-plumber')
except PackageNotFoundError:
    pass