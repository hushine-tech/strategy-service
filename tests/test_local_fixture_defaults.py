from pathlib import Path


def test_local_database_fixture_scripts_default_to_loopback() -> None:
    root = Path(__file__).resolve().parents[1]
    scripts = (
        root / "scripts/seed_test_data.py",
        root / "scripts/seed_reconciliation_test_strategy.py",
        root / "scripts/seed_test_strategies.py",
        root / "scripts/upload_debug_strategies.py",
    )

    for script in scripts:
        source = script.read_text(encoding="utf-8")
        assert "192.168.88.10" not in source, (
            f"{script.name} must require an explicit override for remote databases"
        )
        assert "127.0.0.1" in source, (
            f"{script.name} must be usable against the local stack by default"
        )

    seed_source = scripts[0].read_text(encoding="utf-8")
    assert 'os.environ.get("TIMESCALE_DB", "binance_2025")' in seed_source
