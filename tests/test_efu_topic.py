"""Where an EFU publishes, and who gets to decide."""
from pathlib import Path
import pytest
from mccode_plumber.manage.efu import EventFormationUnitConfig


@pytest.fixture
def files(tmp_path):
    binary = tmp_path / 'bifrost'
    binary.write_text('#!/bin/sh\n')
    binary.chmod(0o755)
    for name in ('bifrost.json', 'bifrost.calib'):
        (tmp_path / name).write_text('{}')
    return {'binary': binary, 'config': tmp_path / 'bifrost.json',
            'calibration': tmp_path / 'bifrost.calib', 'port': 9000}


class TestUnresolved:
    def test_an_absent_topic_stays_absent(self, files):
        # Not 'SimulatedEvents': defaulting here is what let the EFU publish somewhere
        # the filewriter was not subscribed.
        cfg = EventFormationUnitConfig.from_dict(files)
        assert cfg.topic is None and cfg.samples_topic is None

    def test_an_unresolved_config_cannot_reach_the_efu(self, files):
        cfg = EventFormationUnitConfig.from_dict(files)
        with pytest.raises(ValueError, match='has no topic'):
            cfg.to_dict()

    def test_resolving_fills_the_topic_and_its_samples(self, files):
        cfg = EventFormationUnitConfig.from_dict(files).resolve_topic('bifrost_detector')
        assert cfg.topic == 'bifrost_detector'
        assert cfg.samples_topic == 'bifrost_detector_samples'
        assert cfg.to_dict()['topic'] == 'bifrost_detector'


class TestExplicit:
    def test_an_explicit_topic_outranks_the_structure(self, files):
        cfg = EventFormationUnitConfig.from_dict({**files, 'topic': 'chosen'})
        assert cfg.resolve_topic('from_structure').topic == 'chosen'

    def test_an_explicit_samples_topic_survives_resolution(self, files):
        cfg = EventFormationUnitConfig.from_dict({**files, 'samples_topic': 'samples'})
        resolved = cfg.resolve_topic('det')
        assert resolved.topic == 'det' and resolved.samples_topic == 'samples'

    def test_resolving_against_nothing_changes_nothing(self, files):
        cfg = EventFormationUnitConfig.from_dict(files)
        assert cfg.resolve_topic(None) is cfg

    def test_the_other_required_values_are_still_required(self, files):
        del files['port']
        with pytest.raises(ValueError, match='port'):
            EventFormationUnitConfig.from_dict(files)
