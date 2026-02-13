"""Tests for stream_processor.py"""

import pytest
from stream_processor import StreamProcessor, DataTransformer, StreamAnalytics


class TestStreamProcessor:
    def test_create_topic(self):
        sp = StreamProcessor()
        sp.create_topic("orders", partitions=3)
        assert "orders" in sp.topics
        assert len(sp.topics["orders"]) == 3
        assert sp.metrics["topics_count"] == 1

    def test_produce_adds_message(self):
        sp = StreamProcessor()
        sp.create_topic("orders", partitions=2)
        msg_id = sp.produce("orders", {"item": "book"}, partition=1)
        assert msg_id is not None
        assert len(sp.topics["orders"][1]) == 1
        assert sp.topics["orders"][1][0]["data"]["item"] == "book"

    def test_produce_invalid_partition_raises(self):
        sp = StreamProcessor()
        sp.create_topic("orders", partitions=2)
        with pytest.raises(ValueError, match="Invalid partition"):
            sp.produce("orders", {"item": "book"}, partition=5)

    def test_produce_negative_partition_raises(self):
        sp = StreamProcessor()
        sp.create_topic("orders", partitions=2)
        with pytest.raises(ValueError, match="Invalid partition"):
            sp.produce("orders", {"item": "book"}, partition=-1)

    def test_consume_receives_messages(self):
        sp = StreamProcessor()
        sp.create_topic("events", partitions=1)
        received = []
        sp.consume("events", "group1", lambda msg: received.append(msg))
        sp.produce("events", {"type": "click"})
        assert len(received) == 1
        assert received[0]["data"]["type"] == "click"

    def test_list_topics(self):
        sp = StreamProcessor()
        sp.create_topic("a")
        sp.create_topic("b")
        assert sorted(sp.list_topics()) == ["a", "b"]

    def test_get_metrics(self):
        sp = StreamProcessor()
        sp.create_topic("t")
        sp.produce("t", {"x": 1})
        m = sp.get_metrics()
        assert m["messages_processed"] == 1
        assert m["topics_count"] == 1


class TestDataTransformer:
    def test_transform_applies_function(self):
        result = DataTransformer.transform_data(
            {"price": 10},
            lambda d: {**d, "price_with_tax": d["price"] * 1.1},
        )
        assert result["price_with_tax"] == pytest.approx(11.0)

    def test_filter_data(self):
        assert DataTransformer.filter_data({"age": 25}, lambda d: d["age"] > 18)
        assert not DataTransformer.filter_data({"age": 10}, lambda d: d["age"] > 18)

    def test_aggregate_data(self):
        data = [{"v": 1}, {"v": 2}, {"v": 3}]
        total = DataTransformer.aggregate_data(data, lambda ds: sum(d["v"] for d in ds))
        assert total == 6


class TestStreamAnalytics:
    def test_add_and_average(self):
        sa = StreamAnalytics()
        sa.add_data_point("cpu", 50.0)
        sa.add_data_point("cpu", 100.0)
        assert sa.get_average("cpu") == pytest.approx(75.0)

    def test_empty_metric_average(self):
        sa = StreamAnalytics()
        assert sa.get_average("nonexistent") == 0.0

    def test_throughput_with_few_points(self):
        sa = StreamAnalytics()
        sa.add_data_point("x", 1.0)
        assert sa.get_throughput("x") == 0.0

    def test_message_count_tracking(self):
        sa = StreamAnalytics()
        for i in range(5):
            sa.add_data_point("events", 1.0)
        assert len(sa.window_data["events"]) == 5
