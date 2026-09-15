"""What `mp-nexus-services` reads out of the structure before starting anything."""
import json
import sys
import pytest
from mccode_plumber.manage import orchestrate


def stream(module, topic, source):
    return {'module': module, 'config': {'topic': topic, 'source': source}}


@pytest.fixture
def run_services(tmp_path, monkeypatch):
    """Call services() over a written structure, returning the kwargs it hands on."""
    def go(*children, argv_extra=()):
        instrument = tmp_path / 'inst.instr'
        instrument.write_text('')
        (tmp_path / 'inst.json').write_text(json.dumps(
            {'children': [{'type': 'group', 'name': 'entry', 'children': list(children)}]}
        ))
        captured = {}
        monkeypatch.setattr(orchestrate, 'get_instr_name_and_parameters',
                            lambda _f: ('inst', ()))
        monkeypatch.setattr(orchestrate, 'load_in_wait_load_out',
                            lambda **kw: captured.update(kw))
        monkeypatch.setattr(sys, 'argv',
                            ['mp-nexus-services', str(instrument), *argv_extra])
        orchestrate.services()
        return captured
    return go


class TestEventTopic:
    def test_it_comes_from_the_structures_detector(self, run_services):
        kw = run_services(stream('ev44', 'bifrost_detector', 'arc=1;triplet=2'))
        assert kw['event_topic'] == 'bifrost_detector'

    def test_a_structure_without_detectors_derives_nothing(self, run_services):
        kw = run_services(stream('da00', 'bifrost_beam_monitor', 'cbm1'))
        assert kw['event_topic'] is None


class TestTopicRegistration:
    def test_every_structure_topic_is_carried_through(self, run_services):
        kw = run_services(
            stream('ev44', 'bifrost_detector', 'arc=1'),
            stream('da00', 'bifrost_beam_monitor', 'cbm1'),
            stream('f144', 'bifrost_motors', 'a3'),
        )
        assert set(kw['stream_topics']) == {
            'bifrost_detector', 'bifrost_beam_monitor', 'bifrost_motors'
        }

    def test_a_missing_structure_is_not_fatal(self, tmp_path, monkeypatch):
        # services() tolerates an absent JSON; it should still start the services.
        instrument = tmp_path / 'lonely.instr'
        instrument.write_text('')
        captured = {}
        monkeypatch.setattr(orchestrate, 'get_instr_name_and_parameters',
                            lambda _f: ('lonely', ()))
        monkeypatch.setattr(orchestrate, 'load_in_wait_load_out',
                            lambda **kw: captured.update(kw))
        monkeypatch.setattr(sys, 'argv', ['mp-nexus-services', str(instrument)])
        orchestrate.services()
        assert captured['event_topic'] is None
        assert captured['stream_topics'] == []
