from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from .manager import Manager
from .ensure import ensure_executable


@dataclass
class Forwarder(Manager):
    """
    Manage the execution of a Forwarder to send EPICS PV updates to Kafka

    Parameters
    ----------
    broker:     the name or address and port of the broker to which updated
                EPICS values will be sent, once configured. (localhost:9092)
    config:     the broker and topic used for configuring the forwarder
                (localhost:9092/ForwardConfig)
    status:     the broker and topic used for forwarder status messages
                (localhost:9092/ForwardStatus)
    retrieve:   Retrieve values from Kafka at configuration (False == don't)
    verbosity:  Control if (Trace, Debug, Warning, Error, or Critical) messages
                should be printed to STDOUT

    Note
    ----
    `config` and `status` can be provided as _only_ their topic if they use the same
    broker as PV updates. In such a case, there will be no '/' character in their input
    value and `lambda value = f'{broker}/{value}'` will replace them.

    """
    broker: str | None = None
    config: str | None = None
    status: str | None = None
    retrieve: bool = False
    verbosity: str | None = None
    _command: Path = field(default_factory=lambda: Path('forwarder-launch'))
    _broker: str = field(default='localhost:9092')
    _config: str = field(default='ForwardConfig')
    _status: str = field(default='ForwardStatus')

    def __post_init__(self):
        from mccode_plumber.kafka import register_kafka_topics, all_exist
        self._command =ensure_executable(self._command)
        if self.broker is None:
            self.broker = self._broker
        if self.config is None:
            self.config = self._config
        if self.status is None:
            self.status = self._status
        if '/' not in self.config:
            self.config = f'{self.broker}/{self.config}'
        if '/' not in self.status:
            self.status = f'{self.broker}/{self.status}'
        self._broker = self.broker
        self._config = self.config
        self._status = self.status

        for broker_topic in (self.config, self.status):
            b, t = broker_topic.split('/')
            res = register_kafka_topics(b, [t])
            if not all_exist(res.values()):
                raise RuntimeError(f'Missing Kafka topics? {res}')


    def __run_command__(self) -> list[str]:
        args: list[str] = [
            self._command.as_posix(),
            '--config-topic', self._config,
            '--status-topic', self._status,
            '--output-broker', self._broker,
        ]
        if not self.retrieve:
            args.append('--skip-retrieval')
        if (v:=forwarder_verbosity(self.verbosity)) is not None:
            args.extend(['-v', v])
        return args


def forwarder_verbosity(v):
    if isinstance(v, str):
        for k in ('Trace', 'Debug', 'Warning', 'Error', 'Critical'):
            if k.lower() == v.lower():
                return k
    return None