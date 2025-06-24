from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from mccode_plumber.manage.manager import Manager, ensure_path

@dataclass
class EventFormationUnitManager(Manager):
    """
    Command and control of an Event Formation Unit

    Properties
    ----------
    binary: the full path to a binary file which is the EFU
    config: the full path to its configuration JSON file
    calibration: the full path to its calibration JSON file
    broker: the domain name or IP and port of the Kafka broker
    topic: the EV44 detector data Kafka stream topic
    samples_topic: the raw AR51 detector data Kafka stream topic
    port: the UDP port at which the EFU will listen for Readout messages
    command: the TCP port the EFU will use to listen for command messages, e.g. EXIT
    monitor_every: For every `monitor_every`th Readout packet
    monitor_consecutive: Send `monitor_consecutive` raw packets to `samples_topic`
    """
    binary: Path
    config: Path
    calibration: Path
    broker: str | None = None
    topic: str | None = None
    samples_topic: str | None = None
    port: int = 9000
    command: int | None = None
    monitor_every: int = 1000
    monitor_consecutive: int = 2

    def __post_init__(self):
        from os import X_OK, R_OK
        self.binary = ensure_path(self.binary, X_OK)
        self.config = ensure_path(self.config, R_OK)
        self.calibration = ensure_path(self.calibration, R_OK)
        if self.broker is None:
            self.broker = 'localhost:9092'
        if self.topic is None:
            self.topic = self.binary.stem
        if self.samples_topic is None:
            self.samples_topic = f'{self.topic}_samples'

    def __run_command__(self):
        from ephemeral_port_reserve import reserve
        if self.command is None:
            self.command = reserve()
        argv = [self.binary,
                '-b', self.broker,
                '-t', self.topic,
                '--ar51_topic', self.topic,
                '--file', self.config,
                '--calibration', self.calibration,
                '--port', str(self.port),
                '--cmdport', str(self.command),
                '--monitor-every', str(self.monitor_every),
                '--monitor-consecutive', str(self.monitor_consecutive),
                '--nohwcheck']
        return argv

    def finalize(self):
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(('localhost', self.command))
                sock.sendall(bytes("EXIT\n", "utf-8"))
                received = str(sock.recv(1024), "utf-8")
            except ConnectionRefusedError:
                # the server is already dead or was not started?
                received = '<OK>'
        if received != "<OK>":
            print(
                f"EFU responded with {received} when asked to exit. Check your system status manager whether it exited")
