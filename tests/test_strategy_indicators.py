from hushine_strategy.indicator_output import IndicatorWriter as SharedIndicatorWriter
from strategy_service import indicators
from strategy_service.indicators import IndicatorWriter, parse_indicator_definitions


def test_hosted_worker_uses_shared_indicator_writer():
    assert IndicatorWriter is SharedIndicatorWriter


def test_parse_indicator_definitions_defaults_name_and_optional_fields():
    defs = parse_indicator_definitions({
        "alpha_score": {"type": "line", "pane": "strategy"},
        "entry_signal": {"type": "marker", "pane": "price", "name": "Entry Signal"},
    })

    assert defs[0].key == "alpha_score"
    assert defs[0].name == "alpha_score"
    assert defs[0].type == "line"
    assert defs[0].pane == "strategy"
    assert defs[1].name == "Entry Signal"


def test_indicator_writer_rejects_undeclared_key():
    defs = parse_indicator_definitions({"alpha_score": {"type": "line", "pane": "strategy"}})
    writer = IndicatorWriter(defs)

    writer.set("missing", 1.0)

    frame = writer.drain()
    assert frame.values == {}
    assert frame.warnings == ["undeclared indicator key ignored: missing"]


def test_direct_indicator_chunk_buffer_is_removed():
    assert not hasattr(indicators, "IndicatorChunkBuffer")
