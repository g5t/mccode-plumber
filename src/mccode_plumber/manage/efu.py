from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from ephemeral_port_reserve import reserve
from mccode_plumber.manage.manager import Manager, ensure_readable_file, ensure_executable

@dataclass
class EventFormationUnit(Manager):
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
    command: int = field(default_factory=reserve)
    monitor_every: int = 1000
    monitor_consecutive: int = 2

    def __post_init__(self):
        self.binary = ensure_executable(self.binary)
        self.config = ensure_readable_file(self.config)
        self.calibration = ensure_readable_file(self.calibration)
        if self.broker is None:
            self.broker = 'localhost:9092'
        if self.topic is None:
            self.topic = self.binary.stem
        if self.samples_topic is None:
            self.samples_topic = f'{self.topic}_samples'

    def __run_command__(self):
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
        message = f"Check your system status manager whether {self.binary} is active."
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.settimeout(1.0)
                sock.connect(('localhost', self.command))
                sock.sendall(bytes("EXIT\n", "utf-8"))
                received = str(sock.recv(1024), "utf-8")
            except TimeoutError:
                print(f"Communication timed out, is the EFU running? {message}")
                return
            except ConnectionRefusedError:
                # the server is already dead or was not started?
                received = '<OK>'
        if received.strip() != "<OK>":
            print(f"EFU responded '{received.strip()}' when asked to exit. {message}")
