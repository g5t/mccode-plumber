from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from mccode_plumber.manage.manager import Manager


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
    job:        the topic used for receiving jobs (WriterJob)
    verbosity:  the level of output to print to STDOUT, any of
                (trace, debug, info, warning, error, critical)
    """
    broker: str | None = None
    work: Path | None = None
    command: str | None = None
    job: str | None = None
    verbosity: str | None = None
    _command: Path = field(default_factory=lambda: Path('kafka-to-nexus'))

    def __post_init__(self):
        from mccode_plumber.kafka import register_kafka_topics, all_exist
        from mccode_plumber.manage.manager import (
            ensure_writable_directory, ensure_executable
        )
        self._command = ensure_executable(self._command)
        if self.broker is None:
            self.broker = 'localhost:9092'
        if self.command is None:
            self.config = 'WriterCommand'
        if self.job is None:
            self.job = 'WriterJob'
        self.work = ensure_writable_directory(self.work or Path())

        res = register_kafka_topics(self.broker, [self.command, self.job])
        if not all_exist(res.values()):
            raise RuntimeError(f'Missing Kafka topics? {res}')

    def __run_command__(self) -> list[str]:
        args = [
            self._command,
            '--brokers', self.broker,
            '--command-status-topic', self.command,
            '--job-pool-topic', self.job,
            f'--hdf-output-prefix={self.work}/',
            '--kafka-error-timeout', '10s',
            '--kafka-poll-timeout', '1s',
            '--kafka-metadata-max-timeout', '10s',
        ]
        if self.verbosity in ('critical', 'error', 'warning', 'info', 'debug', 'trace'):
            args.extend(['--verbosity', self.verbosity])
        return args
