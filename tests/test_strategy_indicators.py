from strategy_service.indicators import IndicatorChunkBuffer, IndicatorWriter, parse_indicator_definitions


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


def test_chunk_buffer_flushes_1024_values_and_keeps_nulls():
    defs = parse_indicator_definitions({"alpha_score": {"type": "line", "pane": "strategy"}})
    buffer = IndicatorChunkBuffer(defs, chunk_size=1024)
    stream_key = "binance:perpetual_futures:ETHUSDT:1m"

    chunks = []
    for index in range(1024):
        writer = IndicatorWriter(defs)
        if index != 2:
            writer.set("alpha_score", float(index))
        chunks = buffer.record_bar(stream_key, 1_780_000_000_000 + index * 60_000, 60_000, writer.drain())

    assert len(chunks) == 1
    assert chunks[0].indicator_key == "alpha_score"
    assert chunks[0].chunk_index == 0
    assert chunks[0].count == 1024
    assert chunks[0].values_json["values"][2] is None
    assert chunks[0].values_json["values"][1023] == 1023.0


def test_marker_chunk_allows_multiple_markers_at_same_offset():
    defs = parse_indicator_definitions({"signal": {"type": "marker", "pane": "price"}})
    buffer = IndicatorChunkBuffer(defs, chunk_size=2)
    stream_key = "binance:perpetual_futures:ETHUSDT:1m"

    writer = IndicatorWriter(defs)
    writer.mark("signal", text="BUY", price=1580.2, color="#16a34a")
    writer.mark("signal", text="RISK", price=1581.0, color="#d97706")
    assert buffer.record_bar(stream_key, 1_780_000_000_000, 60_000, writer.drain()) == []

    chunks = buffer.record_bar(stream_key, 1_780_000_060_000, 60_000, IndicatorWriter(defs).drain())

    assert len(chunks) == 1
    assert chunks[0].values_json["markers"][0]["offset"] == 0
    assert chunks[0].values_json["markers"][1]["text"] == "RISK"
