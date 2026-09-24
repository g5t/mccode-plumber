"""The module-aware view of a NeXus structure's stream directives."""
from mccode_plumber.manage.orchestrate import (
    EVENT_MODULES, MONITOR_MODULES, Stream, event_topic_from_streams,
    get_stream_modules, get_stream_pairs, sources_by_topic, streams_of_module,
    topics_of,
)
import pytest


def stream(module, topic, source):
    return {'module': module, 'config': {'topic': topic, 'source': source}}


def structure(*children):
    return {'children': [{'type': 'group', 'name': 'entry', 'children': list(children)}]}


class TestHarvest:
    def test_a_directive_is_found_with_its_module(self):
        s = structure(stream('da00', 'mon', 'cbm1'), stream('ev44', 'det', 'arc=1'))
        assert get_stream_modules(s) == [
            Stream('da00', 'mon', 'cbm1'), Stream('ev44', 'det', 'arc=1')
        ]

    def test_nesting_does_not_hide_a_directive(self):
        deep = structure({'type': 'group', 'name': 'instrument', 'children': [
            {'type': 'group', 'name': 'monitor', 'children': [stream('da00', 'mon', 'cbm1')]}
        ]})
        assert get_stream_modules(deep) == [Stream('da00', 'mon', 'cbm1')]

    def test_a_link_is_not_a_stream(self):
        # link mirrors a group another module fills; it has a source but no topic,
        # and subscribing to it would be subscribing to nothing.
        s = structure({'module': 'link', 'config': {'name': 'value', 'source': '/entry/x/value'}})
        assert get_stream_modules(s) == []

    def test_a_dataset_carrying_the_keys_is_not_a_stream(self):
        # The loose 'any dict with topic and source' walk this replaced would have
        # taken this one, and registered a topic that does not exist.
        s = structure({'module': 'dataset', 'config': {
            'name': 'description', 'values': {'topic': 'not-a-topic', 'source': 'not-a-source'}
        }})
        assert get_stream_modules(s) == []

    def test_one_directive_declared_twice_is_one_stream(self):
        s = structure(stream('da00', 'mon', 'cbm1'), stream('da00', 'mon', 'cbm1'))
        assert get_stream_modules(s) == [Stream('da00', 'mon', 'cbm1')]

    def test_pairs_still_answer_the_old_question(self):
        s = structure(stream('da00', 'mon', 'cbm1'), stream('ev44', 'det', 'arc=1'))
        assert get_stream_pairs(s) == [('mon', 'cbm1'), ('det', 'arc=1')]


class TestSelection:
    def test_modules_separate_monitors_from_events(self):
        streams = get_stream_modules(structure(
            stream('da00', 'mon', 'cbm1'), stream('ev44', 'det', 'arc=1'),
            stream('f144', 'params', 'mcstas:a3'),
        ))
        assert topics_of(streams_of_module(streams, MONITOR_MODULES)) == ['mon']
        assert topics_of(streams_of_module(streams, EVENT_MODULES)) == ['det']

    def test_sources_group_under_their_topic(self):
        streams = get_stream_modules(structure(
            stream('da00', 'mon', 'cbm1'), stream('da00', 'mon', 'cbm2'),
            stream('da00', 'other', 'cbm3'),
        ))
        assert sources_by_topic(streams) == {'mon': ['cbm1', 'cbm2'], 'other': ['cbm3']}

    def test_topics_keep_first_appearance_order(self):
        streams = get_stream_modules(structure(
            stream('da00', 'b', 'x'), stream('da00', 'a', 'y'), stream('da00', 'b', 'z'),
        ))
        assert topics_of(streams) == ['b', 'a']


class TestEventTopic:
    def test_the_detector_topic_comes_from_the_structure(self):
        streams = get_stream_modules(structure(
            stream('ev44', 'bifrost_detector', 'arc=1;triplet=2'),
            stream('da00', 'bifrost_beam_monitor', 'cbm1'),
        ))
        assert event_topic_from_streams(streams) == 'bifrost_detector'

    def test_no_event_stream_leaves_the_choice_to_the_caller(self):
        streams = get_stream_modules(structure(stream('da00', 'mon', 'cbm1')))
        assert event_topic_from_streams(streams) is None

    def test_several_event_topics_is_an_error_not_a_guess(self):
        streams = get_stream_modules(structure(
            stream('ev44', 'det_a', 'arc=1'), stream('ev44', 'det_b', 'arc=2'),
        ))
        with pytest.raises(ValueError, match='several topics'):
            event_topic_from_streams(streams)


class TestRealStructure:
    def test_the_teaching_structure_yields_its_monitor(self):
        from json import load
        from pathlib import Path
        path = Path(__file__).parent.parent.parent / 'niess' / 'teaching_nexus_structure.json'
        if not path.exists():
            pytest.skip('niess checkout not beside mccode-plumber')
        streams = get_stream_modules(load(path.open()))
        assert streams == [Stream('da00', 'teaching_beam_monitor', 'monitor')]
        assert event_topic_from_streams(streams) is None
