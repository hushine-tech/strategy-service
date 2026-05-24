from strategy_service.config import Config


def test_apply_env_overrides_uses_core_service_grpc_addr(monkeypatch):
    monkeypatch.setenv("CORE_SERVICE_GRPC_ADDR", "core.internal:50051")

    cfg = Config()
    cfg.apply_env_overrides()

    assert cfg.dependencies.account_service_grpc == "core.internal:50051"


def test_apply_env_overrides_keeps_legacy_account_service_grpc_addr(monkeypatch):
    monkeypatch.setenv("ACCOUNT_SERVICE_GRPC_ADDR", "legacy.internal:50051")

    cfg = Config()
    cfg.apply_env_overrides()

    assert cfg.dependencies.account_service_grpc == "legacy.internal:50051"
