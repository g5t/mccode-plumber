from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from ephemeral_port_reserve import reserve
from .manager import Manager
from .ensure import ensure_readable_file, ensure_executable

@dataclass
class EventFormationUnitConfig:
    name: str
    binary: Path
    config: Path
    calibration: Path
    topic: str
    samples_topic: str
    port: int
    monitor_every: int
    monitor_consecutive: int

    @classmethod
    def from_dict(cls, data: dict):
        required = ('binary', 'config', 'calibration', 'topic', 'port')
        if any(req not in data for req in required):
            msg = [req for req in required if req not in data]
            msg = ', '.join(msg)
            raise ValueError(f"Missing required value{'' if len(msg)==1 else 's'}: {msg}")
        binary = ensure_readable_file(data['binary'])
        config = ensure_readable_file(data['config'])
        calibration = ensure_readable_file(data['calibration'])
        topic = data['topic']
        port = int(data['port'])
        monitor_every = int(data.get('monitor_every', 1000))
        monitor_consecutive = int(data.get('monitor_consecutive', 2))
        name = data.get('name', binary.stem)
        samples_topic = data.get('samples_topic', f'{topic}_samples')
        return cls(name, binary, config, calibration, topic, samples_topic, port, monitor_every, monitor_consecutive)

    def to_dict(self):
        d = {
            'name': self.name,
            'binary': self.binary.as_posix(),
            'config': self.config.as_posix(),
            'calibration': self.calibration.as_posix(),
            'topic': self.topic,
            'samples_topic': self.samples_topic,
            'port': self.port,
            'monitor_every': self.monitor_every,
            'monitor_consecutive': self.monitor_consecutive,
        }
        return d

    def to_cli_str(self):
        from json import dumps
        return dumps(self.to_dict()).translate(str.maketrans('','',' {}"'))

    @classmethod
    def from_cli_str(cls, cli_str: str):
        """Split a command-line argument string into a EventFormationUnitConfig

        Note
        ----
        A command-line argument string is expected to have the following format:
            "arg1:value1,arg2:value2,...,argN:valueN"
        That is, key-value pairs separated from each other by commas and internally
        by a colon. This allows for _values_ which contain colons, but not keys.
        """
        data =dict(z.split(':', maxsplit=1) for z in [x for x in cli_str.split(',')])
        return cls.from_dict(data)



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
        argv = [self.binary.as_posix(),
                '-b', self.broker,
                '-t', self.topic,
                '--ar51_topic', self.samples_topic,
                '--file', self.config.as_posix(),
                '--calibration', self.calibration.as_posix(),
                '--port', str(self.port),
                '--cmdport', str(self.command),
                '--monitor_every', str(self.monitor_every),
                '--monitor_consecutive', str(self.monitor_consecutive),
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
