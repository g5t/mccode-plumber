from __future__ import annotations
from dataclasses import dataclass
from mccode_plumber.manage.manager import Manager, ensure_path


@dataclass
class ForwarderManager(Manager):
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
    forwarder_command: str = 'forwarder-launch'

    def __post_init__(self):
        from os import access, X_OK
        from mccode_plumber.kafka import register_kafka_topics, all_exist
        if not access(self.forwarder_command, X_OK):
            raise ValueError(f'{self.forwarder_command} is not a valid command')
        if self.broker is None:
            self.broker = 'localhost:9092'
        if self.config is None:
            self.config = 'ForwardConfig'
        if self.status is None:
            self.status = 'ForwardStatus'
        if '/' not in self.config:
            self.config = f'{self.broker}/{self.config}'
        if '/' not in self.status:
            self.status = f'{self.broker}/{self.status}'

        for broker_topic in (self.config, self.status):
            b, t = broker_topic.split('/')
            res = register_kafka_topics(b, [t])
            if not all_exist(res.values()):
                raise RuntimeError(f'Missing Kafka topics? {res}')


    def __run_command__(self) -> list[str]:
        args = [
            self.forwarder_command,
            '--config-topic', self.config,
            '--status-topic', self.status,
            '--output-broker', self.broker,
        ]
        if not self.retrieve:
            args.append('--skip-retrieval')
        if self.verbosity in ('Trace', 'Debug', 'Warning', 'Error', 'Critical'):
            args.extend(['-v', self.verbosity])
        return args
