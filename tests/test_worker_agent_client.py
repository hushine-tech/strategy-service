import pytest

from strategy_service.worker_agent_client import WorkerEnv, load_worker_env


def test_load_worker_env_requires_agent_addr(monkeypatch):
    monkeypatch.delenv("HUSHINE_AGENT_ADDR", raising=False)
    monkeypatch.setenv("HUSHINE_WORKER_TOKEN", "token")
    monkeypatch.setenv("HUSHINE_SESSION_ID", "sess-1")

    with pytest.raises(RuntimeError, match="HUSHINE_AGENT_ADDR"):
        load_worker_env()


def test_load_worker_env_accepts_required_values(monkeypatch):
    monkeypatch.setenv("HUSHINE_AGENT_ADDR", "127.0.0.1:50000")
    monkeypatch.setenv("HUSHINE_WORKER_TOKEN", "token")
    monkeypatch.setenv("HUSHINE_SESSION_ID", "sess-1")
    monkeypatch.setenv("HUSHINE_DEBUGPY_PORT", "5678")

    env = load_worker_env()

    assert env == WorkerEnv(
        agent_addr="127.0.0.1:50000",
        token="token",
        session_id="sess-1",
        debugpy_port=5678,
    )
