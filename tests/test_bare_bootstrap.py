from pathlib import Path
from types import SimpleNamespace

from strategy_service import bare_bootstrap
from strategy_service.bare_bootstrap import BareBootstrapResult, write_runtime_mtls_bundle


def test_write_runtime_mtls_bundle_sets_permissions(tmp_path: Path) -> None:
    result = BareBootstrapResult(
        runtime_id="bare-42-test",
        name="bare-debug-test",
        client_cert_pem="client-cert",
        client_key_pem="client-key",
        server_ca_pem="server-ca",
    )

    paths = write_runtime_mtls_bundle(tmp_path, result)

    assert paths.root_cert_file.read_text() == "server-ca"
    assert paths.client_cert_file.read_text() == "client-cert"
    assert paths.client_key_file.read_text() == "client-key"
    assert oct(paths.client_key_file.stat().st_mode & 0o777) == "0o600"


def test_bootstrap_bare_runtime_uses_insecure_channel_by_default(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []

    class FakeChannel:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class FakeStub:
        def __init__(self, _channel):
            pass

        def BootstrapBareRuntimeCertificate(self, _request):
            return SimpleNamespace(
                runtime_id="bare-42-test",
                name="bare-debug-test",
                client_cert_pem="client-cert",
                server_ca_pem="server-ca",
            )

    monkeypatch.setattr(bare_bootstrap, "generate_key_and_csr", lambda _runtime_id: ("client-key", "csr"))
    monkeypatch.setattr(bare_bootstrap.cp_grpc, "ControlPanelServiceStub", FakeStub)
    monkeypatch.setattr(bare_bootstrap.grpc, "insecure_channel", lambda _address: calls.append("insecure") or FakeChannel())
    monkeypatch.setattr(
        bare_bootstrap.grpc,
        "secure_channel",
        lambda *_args, **_kwargs: calls.append("secure") or FakeChannel(),
    )

    paths = bare_bootstrap.bootstrap_bare_runtime_certificate(
        address="127.0.0.1:50054",
        user_id=42,
        runtime_id="bare-42-test",
        name="bare-debug-test",
        root_cert_file="",
        output_dir=tmp_path,
    )

    assert calls == ["insecure"]
    assert paths.client_key_file.read_text() == "client-key"


def test_bootstrap_bare_runtime_can_use_tls(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []
    root = tmp_path / "root.pem"
    root.write_text("server-ca", encoding="utf-8")

    class FakeChannel:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class FakeStub:
        def __init__(self, _channel):
            pass

        def BootstrapBareRuntimeCertificate(self, _request):
            return SimpleNamespace(
                runtime_id="bare-42-test",
                name="bare-debug-test",
                client_cert_pem="client-cert",
                server_ca_pem="server-ca",
            )

    monkeypatch.setattr(bare_bootstrap, "generate_key_and_csr", lambda _runtime_id: ("client-key", "csr"))
    monkeypatch.setattr(bare_bootstrap.cp_grpc, "ControlPanelServiceStub", FakeStub)
    monkeypatch.setattr(bare_bootstrap.grpc, "ssl_channel_credentials", lambda **_kwargs: object())
    monkeypatch.setattr(bare_bootstrap.grpc, "insecure_channel", lambda _address: calls.append("insecure") or FakeChannel())
    monkeypatch.setattr(
        bare_bootstrap.grpc,
        "secure_channel",
        lambda *_args, **_kwargs: calls.append("secure") or FakeChannel(),
    )

    bare_bootstrap.bootstrap_bare_runtime_certificate(
        address="127.0.0.1:50054",
        user_id=42,
        runtime_id="bare-42-test",
        name="bare-debug-test",
        root_cert_file=str(root),
        server_name="runtime-channel.local",
        tls_enabled=True,
        output_dir=tmp_path / "bundle",
    )

    assert calls == ["secure"]
