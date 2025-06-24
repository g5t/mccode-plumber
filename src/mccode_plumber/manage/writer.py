from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from mccode_plumber.manage.manager import Manager, ensure_path


@dataclass
class WriterManager(Manager):
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
    _command: str = 'kafka-to-nexus'

    def __post_init__(self):
        from os import access, X_OK, W_OK
        from mccode_plumber.kafka import register_kafka_topics, all_exist
        if not access(self._command, X_OK):
            raise ValueError(f'{self._command} is not a valid command')
        if self.broker is None:
            self.broker = 'localhost:9092'
        if self.command is None:
            self.config = 'WriterCommand'
        if self.job is None:
            self.job = 'WriterJob'
        self.work = ensure_path(self.work or Path(), W_OK, is_dir=True)

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
