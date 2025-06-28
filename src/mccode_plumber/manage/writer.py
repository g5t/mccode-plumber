from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from .manager import Manager
from .ensure import ensure_writable_directory, ensure_executable


@dataclass
class KafkaToNexus(Manager):
    """
    Manage the execution of a kafka-to-nexus file writer

    Parameters
    ----------
    broker:     the name or address and port of the broker containing the needed
                command and job topics (localhost:9092)
    work:       the working directory for file output (`Path()`)
    command:    the topic used for receiving commands (WriterCommand)
    pool:        the topic used for receiving jobs as part of a pool (WriterJob)
    verbosity:  the level of output to print to STDOUT, any of
                (trace, debug, info, warning, error, critical)
    """
    broker: str
    command: str
    pool: str
    work: Path | None = None
    verbosity: str | None = None
    _command: Path = field(default_factory=lambda: Path('kafka-to-nexus'))

    def __post_init__(self):
        from mccode_plumber.kafka import register_kafka_topics, all_exist
        self._command = ensure_executable(self._command)
        self.work = ensure_writable_directory(self.work or Path()).resolve()
        res = register_kafka_topics(self.broker, [self.command, self.pool])
        if not all_exist(res.values()):
            raise RuntimeError(f'Missing Kafka topics? {res}')

    def __run_command__(self) -> list[str]:
        args = [
            self._command.as_posix(),
            '--brokers', f"{self.broker},{self.broker}",
            '--command-status-topic', self.command,
            '--job-pool-topic', self.pool,
            #'--service-name', 'mpw',
            f'--hdf-output-prefix={self.work}/',
            '--kafka-error-timeout', '10s',
            '--kafka-metadata-max-timeout', '10s',
            '--time-before-start', '10s',
        ]
        if (v := writer_verbosity(self.verbosity)) is not None:
            args.extend(['--verbosity', v])
        return args


def writer_verbosity(v):
    if isinstance(v, str):
        for k in ('critical', 'error', 'warning', 'info', 'debug', 'trace'):
            if k.lower() == v.lower():
                return k
    return None