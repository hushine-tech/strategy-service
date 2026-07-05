from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import subprocess
import sys

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from strategy_service.cli.hushine_runtime import (
    _bare_runtime_id,
    _bare_runtime_name,
    _bootstrap_bare_runtime_mtls_if_needed,
    _force_runtime_channel_boundary,
    _materialize_tls_bundle,
)
from strategy_service.config import Config


def _runtime_client_cert(runtime_id: str) -> str:
    now = datetime.now(timezone.utc)
    key = ec.generate_private_key(ec.SECP256R1())
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, runtime_id)]))
        .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-ca")]))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode("utf-8")


def test_runtime_channel_startup_ignores_internal_platform_dependencies():
    cfg = Config()
    cfg.dependencies.portfolio_service_grpc = "127.0.0.1:50051"
    cfg.dependencies.order_service_grpc = "127.0.0.1:50051"
    cfg.dependencies.control_panel_service_grpc = "127.0.0.1:50054"
    cfg.dependencies.runtime_channel_grpc = "127.0.0.1:50055"
    cfg.dependencies.market_data_control_panel_grpc = "127.0.0.1:50054"
    cfg.kafka.brokers = "192.168.88.10:19092"
    cfg.log.kafka_enabled = True
    cfg.log.kafka_brokers = ["192.168.88.10:19092"]
    cfg.database.host = "192.168.88.10"
    cfg.database.database = "binance_{year}"

    _force_runtime_channel_boundary(cfg)

    assert cfg.dependencies.portfolio_service_grpc == ""
    assert cfg.dependencies.order_service_grpc == ""
    assert cfg.dependencies.control_panel_service_grpc == ""
    assert cfg.dependencies.runtime_channel_grpc == "127.0.0.1:50055"
    assert cfg.dependencies.market_data_control_panel_grpc == ""
    assert cfg.kafka.brokers == ""
    assert cfg.log.kafka_enabled is False
    assert cfg.log.kafka_brokers == []
    assert cfg.database.host == ""
    assert cfg.database.database == ""


def test_runtime_channel_addr_has_dedicated_config_and_env(monkeypatch, tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        """
dependencies:
  control_panel_service_grpc: "127.0.0.1:50054"
  runtime_channel_grpc: "127.0.0.1:50055"
""",
        encoding="utf-8",
    )
    cfg = Config.load(str(path))

    assert cfg.dependencies.control_panel_service_grpc == "127.0.0.1:50054"
    assert cfg.dependencies.runtime_channel_grpc == "127.0.0.1:50055"

    monkeypatch.setenv("RUNTIME_CHANNEL_GRPC_ADDR", "runtime-channel.example:50055")
    monkeypatch.setenv("CONTROL_PANEL_SERVICE_GRPC_ADDR", "control-panel.internal:50054")
    cfg.apply_env_overrides()

    assert cfg.dependencies.control_panel_service_grpc == "control-panel.internal:50054"
    assert cfg.dependencies.runtime_channel_grpc == "runtime-channel.example:50055"


def test_market_data_fallback_uses_control_panel_not_runtime_channel(monkeypatch):
    cfg = Config()
    cfg.dependencies.control_panel_service_grpc = ""
    cfg.dependencies.market_data_control_panel_grpc = ""
    monkeypatch.setenv("RUNTIME_CHANNEL_GRPC_ADDR", "runtime-channel.example:50055")

    cfg.apply_env_overrides()

    assert cfg.dependencies.runtime_channel_grpc == "runtime-channel.example:50055"
    assert cfg.dependencies.market_data_control_panel_grpc == ""


def test_runtime_channel_tls_env_overrides(monkeypatch):
    cfg = Config()
    monkeypatch.setenv("RUNTIME_CHANNEL_TLS_ENABLED", "true")
    monkeypatch.setenv("RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE", "/tmp/ca.pem")
    monkeypatch.setenv("RUNTIME_CHANNEL_TLS_SERVER_NAME", "runtime-channel.local")
    monkeypatch.setenv("RUNTIME_CHANNEL_TLS_CLIENT_CERT_FILE", "/tmp/client.pem")
    monkeypatch.setenv("RUNTIME_CHANNEL_TLS_CLIENT_KEY_FILE", "/tmp/client.key")
    monkeypatch.setenv(
        "RUNTIME_CHANNEL_TLS_BUNDLE_JSON",
        '{"client_cert_pem":"cert","client_key_pem":"key","server_ca_pem":"ca"}',
    )

    cfg.apply_env_overrides()

    assert cfg.runtime_channel_tls.enabled is True
    assert cfg.runtime_channel_tls.root_cert_file == "/tmp/ca.pem"
    assert cfg.runtime_channel_tls.server_name == "runtime-channel.local"
    assert cfg.runtime_channel_tls.client_cert_file == "/tmp/client.pem"
    assert cfg.runtime_channel_tls.client_key_file == "/tmp/client.key"
    assert cfg.runtime_channel_tls.bundle_json == '{"client_cert_pem":"cert","client_key_pem":"key","server_ca_pem":"ca"}'


def test_runtime_channel_tls_bundle_can_be_materialized_from_credential_file(monkeypatch, tmp_path):
    cred = tmp_path / "runtime.cred"
    cred.write_text(
        """
{
  "version": 1,
  "key_id": "key-1",
  "private_key_pem": "private-key",
  "client_cert_pem": "client-cert",
  "client_key_pem": "client-key",
  "server_ca_pem": "server-ca"
}
""",
        encoding="utf-8",
    )
    bundle_dir = tmp_path / "tls"
    monkeypatch.setenv("RUNTIME_CHANNEL_TLS_BUNDLE_DIR", str(bundle_dir))
    cfg = Config()
    cfg.runtime.credential_path = str(cred)

    _materialize_tls_bundle(cfg)

    assert cfg.runtime_channel_tls.enabled is True
    assert cfg.runtime_channel_tls.server_name == "runtime-channel.local"
    assert Path(cfg.runtime_channel_tls.root_cert_file).read_text(encoding="utf-8") == "server-ca"
    assert Path(cfg.runtime_channel_tls.client_cert_file).read_text(encoding="utf-8") == "client-cert"
    assert Path(cfg.runtime_channel_tls.client_key_file).read_text(encoding="utf-8") == "client-key"


def test_bare_runtime_id_and_name_can_be_configured():
    assert _bare_runtime_id("desk-runtime", 42) == "desk-runtime"
    assert _bare_runtime_name("desk-debug", 42) == "desk-debug"


def test_bare_runtime_id_and_name_are_generated_when_empty():
    runtime_id = _bare_runtime_id("", 42)
    runtime_name = _bare_runtime_name("", 42)

    assert runtime_id.startswith("bare-42-")
    assert runtime_name.startswith("bare-debug-42-")


def test_bare_runtime_bootstraps_mtls_when_client_cert_missing(monkeypatch, tmp_path):
    cfg = Config()
    cfg.runtime_channel_tls.enabled = True
    cfg.runtime_channel_tls.root_cert_file = "/tmp/control-panel-ca.pem"
    cfg.runtime_channel_tls.server_name = "runtime-channel.local"
    calls = []

    class Paths:
        root_cert_file = tmp_path / "ca.pem"
        client_cert_file = tmp_path / "client.pem"
        client_key_file = tmp_path / "client.key"

    def fake_bootstrap(**kwargs):
        calls.append(kwargs)
        return Paths()

    monkeypatch.setattr(
        "strategy_service.bare_bootstrap.bootstrap_bare_runtime_certificate",
        fake_bootstrap,
    )

    _bootstrap_bare_runtime_mtls_if_needed(
        cfg,
        cp_addr="control-panel.local:50054",
        bare_user_id=42,
        runtime_id="bare-42-debug",
        runtime_name="bare-debug",
        cache_dir=tmp_path / "cache",
    )

    assert calls == [
        {
            "address": "control-panel.local:50054",
            "user_id": 42,
            "runtime_id": "bare-42-debug",
            "name": "bare-debug",
            "root_cert_file": "/tmp/control-panel-ca.pem",
            "server_name": "runtime-channel.local",
            "tls_enabled": False,
            "output_dir": tmp_path / "cache",
        }
    ]
    assert cfg.runtime_channel_tls.root_cert_file == str(Paths.root_cert_file)
    assert cfg.runtime_channel_tls.client_cert_file == str(Paths.client_cert_file)
    assert cfg.runtime_channel_tls.client_key_file == str(Paths.client_key_file)


def test_bare_runtime_reuses_cached_mtls_bundle(monkeypatch, tmp_path):
    cfg = Config()
    cfg.runtime_channel_tls.enabled = True
    cfg.runtime_channel_tls.root_cert_file = "/tmp/control-panel-ca.pem"
    cfg.runtime_channel_tls.server_name = "runtime-channel.local"
    cache = tmp_path / "cache"
    cache.mkdir()
    root = cache / "control-panel-ca.pem"
    cert = cache / "runtime-client.pem"
    key = cache / "runtime-client.key"
    root.write_text("server-ca", encoding="utf-8")
    cert.write_text(_runtime_client_cert("bare-42-debug"), encoding="utf-8")
    key.write_text("client-key", encoding="utf-8")

    def unexpected_bootstrap(**_kwargs):
        raise AssertionError("cached bare mTLS bundle should be reused")

    monkeypatch.setattr(
        "strategy_service.bare_bootstrap.bootstrap_bare_runtime_certificate",
        unexpected_bootstrap,
    )

    _bootstrap_bare_runtime_mtls_if_needed(
        cfg,
        cp_addr="control-panel.local:50054",
        bare_user_id=42,
        runtime_id="bare-42-debug",
        runtime_name="bare-debug",
        cache_dir=cache,
    )

    assert cfg.runtime_channel_tls.root_cert_file == str(root)
    assert cfg.runtime_channel_tls.client_cert_file == str(cert)
    assert cfg.runtime_channel_tls.client_key_file == str(key)


def test_bare_runtime_bootstrap_can_opt_into_tls(monkeypatch, tmp_path):
    cfg = Config()
    cfg.runtime_channel_tls.enabled = True
    cfg.runtime_channel_tls.root_cert_file = "/tmp/control-panel-ca.pem"
    cfg.runtime_channel_tls.server_name = "runtime-channel.local"
    calls = []

    class Paths:
        root_cert_file = tmp_path / "ca.pem"
        client_cert_file = tmp_path / "client.pem"
        client_key_file = tmp_path / "client.key"

    def fake_bootstrap(**kwargs):
        calls.append(kwargs)
        return Paths()

    monkeypatch.setenv("RUNTIME_BARE_BOOTSTRAP_TLS_ENABLED", "true")
    monkeypatch.setattr(
        "strategy_service.bare_bootstrap.bootstrap_bare_runtime_certificate",
        fake_bootstrap,
    )

    _bootstrap_bare_runtime_mtls_if_needed(
        cfg,
        cp_addr="control-panel.local:50054",
        bare_user_id=42,
        runtime_id="bare-42-debug",
        runtime_name="bare-debug",
        cache_dir=tmp_path / "cache",
    )

    assert calls[0]["tls_enabled"] is True


def test_hushine_runtime_cli_module_entrypoint_supports_help():
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    paths = [str(root), str(root / "strategy-library")]
    if env.get("PYTHONPATH"):
        paths.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(paths)

    result = subprocess.run(
        [sys.executable, "-m", "hushine_runtime_cli", "start", "--help"],
        cwd=root,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "usage: hushine-runtime start" in result.stdout
    assert "--runtime-channel-addr" in result.stdout
    assert "--control-panel-addr" in result.stdout


def test_executor_dockerfile_builds_uv_environment_for_module_entrypoint():
    root = Path(__file__).resolve().parents[1]
    dockerfile = (root / "Dockerfile").read_text()

    lock_idx = dockerfile.index("COPY strategy-service/uv.lock")
    copy_idx = dockerfile.index("COPY strategy-service/hushine_runtime_cli.py")
    sync_idx = dockerfile.index("uv sync --frozen --no-dev")

    assert lock_idx < sync_idx
    assert copy_idx < sync_idx
    assert (
        'CMD ["uv", "run", "--no-sync", "python", "-m", "hushine_runtime_cli", "start", "--config", "config.yaml"]'
        in dockerfile
    )
