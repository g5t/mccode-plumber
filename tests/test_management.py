import os
import time
import pytest
from pathlib import Path
from ephemeral_port_reserve import reserve
from mccode_plumber.manage import EventFormationUnit


def command_args(*args):
    largs = [a if isinstance(a, str) else str(a) for a in args]
    if os.name == 'nt':
        return ["python"] + largs
    return largs


def assert_faker_working(port, message, retries=5, timeout=0.1):
    from subprocess import run
    binary = Path(__file__).resolve().parent / 'fake_manager.py'
    res = 'Not-run yet'
    for _ in range(retries):
        res = run(command_args(binary, port, message), capture_output=True, text=True)
        if res.returncode == 0:
            return res.stdout.strip() == message
        time.sleep(timeout)
    print(f'Failed after {retries} attempts: {res}')
    return False


def test_all_fake():
    from subprocess import Popen
    port = reserve()
    binary = Path(__file__).resolve().parent / 'fake_efu.py'
    proc = Popen(command_args(binary, '--cmdport', port))
    time.sleep(0.1) # startup
    assert proc.poll() is None # ensure it's still running
    assert assert_faker_working(port, "fake it till you make it")
    proc.terminate()
    proc.wait()
    assert proc.poll() is not None


@pytest.mark.skipif(os.name == 'nt', reason="fake_efu.py needs the python")
def test_efu_management():
    service = EventFormationUnit.start(
        binary=Path(__file__).parent / "fake_efu.py",
        config=Path(__file__),
        calibration=Path(__file__),
        broker='localhost:9092',
        topic='TestEvents',
        samples_topic='TestEvents_samples',
        port=-1,
        command=reserve(),
        name='efu'
    )
    assert service.poll()
    assert assert_faker_working(service.command, "test_management")
    service.stop()


@pytest.mark.skipif(os.name == 'nt', reason="fake_efu.py needs the python")
def test_minimal_efu_management():
    service = EventFormationUnit.start(
        binary=Path(__file__).parent / "fake_efu.py",
        config=Path(__file__),
        calibration=Path(__file__),
    )
    assert service.poll()
    assert assert_faker_working(service.command, "test_management")
    service.stop()
