from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import grpc
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from strategy_service.gen import control_panel_service_pb2 as cp_pb2
from strategy_service.gen import control_panel_service_pb2_grpc as cp_grpc


@dataclass(frozen=True)
class BareBootstrapResult:
    runtime_id: str
    name: str
    client_cert_pem: str
    client_key_pem: str
    server_ca_pem: str


@dataclass(frozen=True)
class RuntimeMTLSPaths:
    root_cert_file: Path
    client_cert_file: Path
    client_key_file: Path


def generate_key_and_csr(runtime_id: str) -> tuple[str, str]:
    key = ec.generate_private_key(ec.SECP256R1())
    csr = (
        x509.CertificateSigningRequestBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, runtime_id)]))
        .sign(key, hashes.SHA256())
    )
    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    csr_pem = csr.public_bytes(serialization.Encoding.PEM).decode("utf-8")
    return key_pem, csr_pem


def write_runtime_mtls_bundle(directory: Path, result: BareBootstrapResult) -> RuntimeMTLSPaths:
    directory.mkdir(parents=True, exist_ok=True)
    root = directory / "control-panel-ca.pem"
    cert = directory / "runtime-client.pem"
    key = directory / "runtime-client.key"
    root.write_text(result.server_ca_pem, encoding="utf-8")
    cert.write_text(result.client_cert_pem, encoding="utf-8")
    key.write_text(result.client_key_pem, encoding="utf-8")
    key.chmod(0o600)
    return RuntimeMTLSPaths(root_cert_file=root, client_cert_file=cert, client_key_file=key)


def load_existing_runtime_mtls_bundle(directory: Path, *, runtime_id: str) -> RuntimeMTLSPaths | None:
    root = directory / "control-panel-ca.pem"
    cert = directory / "runtime-client.pem"
    key = directory / "runtime-client.key"
    if not all(_has_content(path) for path in (root, cert, key)):
        return None
    if not _client_cert_matches_runtime(cert, runtime_id):
        return None
    return RuntimeMTLSPaths(root_cert_file=root, client_cert_file=cert, client_key_file=key)


def bootstrap_bare_runtime_certificate(
    *,
    address: str,
    user_id: int,
    runtime_id: str,
    name: str,
    root_cert_file: str,
    server_name: str = "",
    tls_enabled: bool = False,
    output_dir: Path,
) -> RuntimeMTLSPaths:
    client_key_pem, csr_pem = generate_key_and_csr(runtime_id)
    options: list[tuple[str, str]] = []
    if tls_enabled and server_name:
        options.append(("grpc.ssl_target_name_override", server_name))
        options.append(("grpc.default_authority", server_name))
    if tls_enabled:
        root_certificates = None
        if root_cert_file:
            root_certificates = Path(root_cert_file).read_bytes()
        credentials = grpc.ssl_channel_credentials(root_certificates=root_certificates)
        channel_factory = lambda: grpc.secure_channel(address, credentials, options=options)
    else:
        channel_factory = lambda: grpc.insecure_channel(address)
    with channel_factory() as channel:
        stub = cp_grpc.ControlPanelServiceStub(channel)
        resp = stub.BootstrapBareRuntimeCertificate(
            cp_pb2.BootstrapBareRuntimeCertificateRequest(
                user_id=int(user_id),
                runtime_id=runtime_id,
                name=name,
                csr_pem=csr_pem,
            )
        )
    return write_runtime_mtls_bundle(
        output_dir,
        BareBootstrapResult(
            runtime_id=resp.runtime_id,
            name=resp.name,
            client_cert_pem=resp.client_cert_pem,
            client_key_pem=client_key_pem,
            server_ca_pem=resp.server_ca_pem,
        ),
    )


def _has_content(path: Path) -> bool:
    try:
        return path.is_file() and path.stat().st_size > 0
    except OSError:
        return False


def _client_cert_matches_runtime(path: Path, runtime_id: str) -> bool:
    runtime_id = str(runtime_id or "").strip()
    if not runtime_id:
        return False
    try:
        cert = x509.load_pem_x509_certificate(path.read_bytes())
    except Exception:  # noqa: BLE001
        return False
    names = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    if not names or names[0].value != runtime_id:
        return False
    now = datetime.now(timezone.utc)
    not_before = getattr(cert, "not_valid_before_utc", None)
    if not_before is None:
        not_before = cert.not_valid_before.replace(tzinfo=timezone.utc)
    not_after = getattr(cert, "not_valid_after_utc", None)
    if not_after is None:
        not_after = cert.not_valid_after.replace(tzinfo=timezone.utc)
    return not_before <= now and not_after > now + timedelta(minutes=1)
