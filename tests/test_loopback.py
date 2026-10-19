"""EPICS stays on the loopback interface: see mccode_plumber.pva."""
from dataclasses import dataclass


def test_a_loopback_client_finds_a_loopback_server():
    from p4p.nt import NTScalar
    from p4p.server import Server, StaticProvider
    from p4p.server.thread import SharedPV
    from mccode_plumber.pva import SERVING, context
    provider = StaticProvider('test')
    provider.add('mccode_plumber:loopback:test', SharedPV(nt=NTScalar('d'), initial=4.5))
    with Server(providers=[provider], **SERVING) as server:
        assert server.conf()['EPICS_PVAS_INTF_ADDR_LIST'].startswith('127.0.0.1')
        assert server.conf()['EPICS_PVAS_BEACON_ADDR_LIST'].startswith('127.0.0.1')
        client = context()
        try:
            assert client.conf()['EPICS_PVA_ADDR_LIST'].startswith('127.0.0.1')
            assert client.get('mccode_plumber:loopback:test', timeout=5) == 4.5
        finally:
            client.close()


def test_started_processes_search_only_this_machine(monkeypatch):
    from mccode_plumber.manage import manager
    from mccode_plumber.pva import ENV
    seen = {}

    class Recorded:
        stdout = stderr = None

        def __init__(self, argv, env=None, **kwargs):
            seen.update(env or {})

    @dataclass
    class Echo(manager.Manager):
        def __run_command__(self):
            return ['true']

    monkeypatch.setattr(manager, 'Popen', Recorded)
    monkeypatch.setattr(manager, 'Thread', lambda **kwargs: type('T', (), {'start': lambda s: None})())
    monkeypatch.setenv('EPICS_PVA_ADDR_LIST', '10.0.0.255')
    Echo.start(name='echo')
    assert {k: seen[k] for k in ENV} == ENV
