"""Sending each monitor's histogram to the topic the structure names."""
import pytest
from mccode_plumber.splitrun import monitors_to_kafka_callback_for_topics


@pytest.fixture
def sent(monkeypatch):
    calls = []

    def fake_send(root, name, topic, source, broker):
        calls.append({'root': root, 'name': name, 'topic': topic, 'source': source})

    monkeypatch.setattr('mccode_plumber.splitrun._send_histogram', fake_send)
    return calls


def by_topic(calls):
    grouped = {}
    for c in calls:
        grouped.setdefault(c['topic'], []).append(c['name'])
    return grouped


@pytest.fixture
def histograms(tmp_path):
    for name in ('cbm1', 'cbm2', 'cbm3'):
        (tmp_path / f'{name}.dat').write_text('')
    return tmp_path


class TestRouting:
    def test_each_topic_gets_its_own_monitors(self, sent, histograms):
        callback, args = monitors_to_kafka_callback_for_topics(
            'localhost:9092', {'mon_a': ['cbm1'], 'mon_b': ['cbm2', 'cbm3']}
        )
        callback(root=histograms)
        assert by_topic(sent) == {'mon_a': ['cbm1'], 'mon_b': ['cbm2', 'cbm3']}

    def test_the_directory_is_passed_as_root(self, sent, histograms):
        callback, args = monitors_to_kafka_callback_for_topics(
            'localhost:9092', {'mon': ['cbm1']}
        )
        assert args == {'dir': 'root'}
        callback(root=histograms)
        assert sent[0]['root'] == histograms

    def test_a_file_root_sends_that_histogram(self, sent, histograms):
        callback, _ = monitors_to_kafka_callback_for_topics('localhost:9092', {'mon': []})
        callback(root=histograms / 'cbm2.dat')
        assert [(c['root'], c['name']) for c in sent] == [(histograms, 'cbm2')]

    def test_an_empty_name_list_takes_every_histogram(self, sent, histograms):
        # The fallback for a structure that declares no monitor stream. An empty list
        # reaching send_histograms verbatim would send nothing at all.
        callback, _ = monitors_to_kafka_callback_for_topics(
            'localhost:9092', {'mon': []}
        )
        callback(root=histograms)
        assert sorted(c['name'] for c in sent) == ['cbm1', 'cbm2', 'cbm3']

    def test_a_list_publishes_each_monitor_under_its_own_name(self, sent, histograms):
        callback, _ = monitors_to_kafka_callback_for_topics('localhost:9092', {'mon': ['cbm1']})
        callback(root=histograms)
        assert [(c['name'], c['source']) for c in sent] == [('cbm1', 'cbm1')]

    def test_a_monitor_publishes_under_the_source_it_is_given(self, sent, histograms):
        # ECDC's structures fill psc_monitor from cbm1: the histogram keeps its name.
        callback, _ = monitors_to_kafka_callback_for_topics(
            'localhost:9092', {'mon': {'cbm1': 'beam_monitor_1'}}
        )
        callback(root=histograms)
        assert [(c['name'], c['source']) for c in sent] == [('cbm1', 'beam_monitor_1')]
        assert not (histograms / 'cbm1.dat').exists()


class TestRemoval:
    def test_files_are_gone_once_every_topic_has_sent(self, sent, histograms):
        callback, _ = monitors_to_kafka_callback_for_topics(
            'localhost:9092', {'mon_a': ['cbm1'], 'mon_b': ['cbm2']}
        )
        callback(root=histograms)
        assert not (histograms / 'cbm1.dat').exists()
        assert not (histograms / 'cbm2.dat').exists()
        assert (histograms / 'cbm3.dat').exists()  # on no topic, so untouched

    def test_a_monitor_on_two_topics_survives_until_both_have_sent(self, sent, histograms):
        # send_histograms(remove=True) deletes what that call sent, so per-call removal
        # would leave the second topic with nothing to read.
        callback, _ = monitors_to_kafka_callback_for_topics(
            'localhost:9092', {'mon_a': ['cbm1'], 'mon_b': ['cbm1']}
        )
        callback(root=histograms)
        assert by_topic(sent) == {'mon_a': ['cbm1'], 'mon_b': ['cbm1']}
        assert not (histograms / 'cbm1.dat').exists()

    def test_removal_can_be_declined(self, sent, histograms):
        callback, _ = monitors_to_kafka_callback_for_topics(
            'localhost:9092', {'mon': ['cbm1']}, delete_after_sending=False
        )
        callback(root=histograms)
        assert (histograms / 'cbm1.dat').exists()
