"""Every EPICS server and client mccode-plumber runs stays on the loopback interface.

The simulated PVs can share names with a real instrument's. EPICS finds a PV by name,
asking whichever servers hear its search, so a search that left this machine could
put a simulated value to a real IOC, or record a real PV as a simulated one. Searches
and beacons sent to 127.0.0.1, by servers listening only there, cannot leave it.

The p4p servers and clients here take `SERVING` and `context()`, which ignore the
environment; the processes mccode-plumber starts (the forwarder among them) get `ENV`.
"""
SERVER = {
    'EPICS_PVAS_INTF_ADDR_LIST': '127.0.0.1',
    'EPICS_PVAS_AUTO_BEACON_ADDR_LIST': 'NO',
    'EPICS_PVAS_BEACON_ADDR_LIST': '127.0.0.1',
}
CLIENT = {
    'EPICS_PVA_AUTO_ADDR_LIST': 'NO',
    'EPICS_PVA_ADDR_LIST': '127.0.0.1',
    'EPICS_PVA_NAME_SERVERS': '',
}
#: For processes started here; Channel Access too, in case one speaks it.
ENV = SERVER | CLIENT | {
    'EPICS_CAS_INTF_ADDR_LIST': '127.0.0.1',
    'EPICS_CAS_AUTO_BEACON_ADDR_LIST': 'NO',
    'EPICS_CAS_BEACON_ADDR_LIST': '127.0.0.1',
    'EPICS_CA_AUTO_ADDR_LIST': 'NO',
    'EPICS_CA_ADDR_LIST': '127.0.0.1',
    'EPICS_CA_NAME_SERVERS': '',
}
#: Keyword arguments for every `p4p.server.Server`.
SERVING = {'conf': SERVER, 'useenv': False}


def context():
    """A PV Access client that searches only this machine."""
    from p4p.client.thread import Context
    return Context('pva', conf=CLIENT, useenv=False)
