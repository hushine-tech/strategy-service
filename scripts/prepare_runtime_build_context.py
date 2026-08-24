#!/usr/bin/env python3
"""Create a sealed Docker build context from exact runtime source worktrees."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import sys
import tarfile


MANIFEST_NAME = ".hushine-runtime-source-manifest.json"


class ContextError(RuntimeError):
    pass


@dataclass(frozen=True)
class Repository:
    name: str
    root: Path
    commit: str
    dirty: bool


def _git(repository: Path, *arguments: str, text: bool = False):
    try:
        return subprocess.run(
            ["git", "-C", str(repository), *arguments],
            check=False,
            capture_output=True,
            text=text,
            encoding="utf-8" if text else None,
            timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ContextError(f"cannot inspect {repository.name} Git repository") from error


def _require_repository(name: str, value: Path) -> Repository:
    root = value.resolve(strict=True)
    top = _git(root, "rev-parse", "--show-toplevel", text=True)
    if top.returncode != 0 or Path(top.stdout.strip()).resolve() != root:
        raise ContextError(f"{name} must be an exact Git repository root")
    commit_result = _git(root, "rev-parse", "--verify", "HEAD^{commit}", text=True)
    if commit_result.returncode != 0:
        raise ContextError(f"{name} has no resolvable HEAD commit")
    commit = commit_result.stdout.strip()
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise ContextError(f"{name} returned an invalid HEAD commit")

    modes = _git(root, "ls-files", "--stage", "-z")
    if modes.returncode != 0:
        raise ContextError(f"cannot inspect {name} tracked paths")
    for entry in modes.stdout.split(b"\0"):
        if entry and entry.startswith(b"160000 "):
            raise ContextError(f"{name} contains an unsupported gitlink or submodule")

    status_result = _git(
        root,
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
    )
    if status_result.returncode != 0:
        raise ContextError(f"cannot inspect {name} worktree state")
    return Repository(name=name, root=root, commit=commit, dirty=bool(status_result.stdout))


def _safe_relative(raw: str) -> PurePosixPath:
    try:
        path = PurePosixPath(raw)
    except Exception as error:
        raise ContextError("Git returned an invalid repository path") from error
    if not raw or path.is_absolute() or "\0" in raw or any(part in {"", ".", ".."} for part in path.parts):
        raise ContextError("Git path escapes its repository")
    return path


def _safe_symlink_target(path: PurePosixPath, target: str) -> None:
    candidate = PurePosixPath(target)
    if not target or candidate.is_absolute() or "\0" in target:
        raise ContextError(f"unsafe symlink target for {path.as_posix()}")
    # The repository is itself one directory below the sealed context root.
    # A tracked sibling link such as strategy-service/strategy-library ->
    # ../strategy-library therefore remains inside the sealed input set.
    depth = len(path.parent.parts) + 1
    for part in candidate.parts:
        if part in {"", "."}:
            continue
        if part == "..":
            depth -= 1
            if depth < 0:
                raise ContextError(f"symlink target escapes repository: {path.as_posix()}")
        else:
            depth += 1


def _destination(root: Path, relative: PurePosixPath) -> Path:
    destination = root.joinpath(*relative.parts)
    try:
        destination.relative_to(root)
    except ValueError as error:
        raise ContextError("staged path escapes Docker context") from error
    return destination


def _remove_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.exists():
        shutil.rmtree(path)


def _write_regular(destination: Path, content: bytes, executable: bool) -> None:
    _remove_path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)
    destination.chmod(0o755 if executable else 0o644)


def _write_symlink(destination: Path, relative: PurePosixPath, target: str) -> None:
    _safe_symlink_target(relative, target)
    _remove_path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.symlink_to(target)


def _export_head(repository: Repository, destination: Path) -> set[str]:
    archive = _git(repository.root, "archive", "--format=tar", repository.commit)
    if archive.returncode != 0:
        raise ContextError(f"cannot export {repository.name} HEAD")
    exported: set[str] = set()
    try:
        with tarfile.open(fileobj=io.BytesIO(archive.stdout), mode="r:") as tar:
            for member in tar:
                relative = _safe_relative(member.name.rstrip("/"))
                target = _destination(destination, relative)
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                    target.chmod(0o755)
                    continue
                if member.isfile():
                    extracted = tar.extractfile(member)
                    if extracted is None:
                        raise ContextError("Git archive contains an unreadable file")
                    _write_regular(target, extracted.read(), bool(member.mode & 0o111))
                elif member.issym():
                    _write_symlink(target, relative, member.linkname)
                else:
                    raise ContextError(f"unsupported Git archive entry: {member.name}")
                exported.add(relative.as_posix())
    except (tarfile.TarError, OSError, UnicodeError) as error:
        raise ContextError(f"cannot unpack {repository.name} Git archive") from error
    return exported


def _listed_worktree_paths(repository: Repository) -> set[str]:
    result = _git(
        repository.root,
        "ls-files",
        "-z",
        "--cached",
        "--others",
        "--exclude-standard",
    )
    if result.returncode != 0:
        raise ContextError(f"cannot enumerate {repository.name} worktree")
    paths: set[str] = set()
    for item in result.stdout.split(b"\0"):
        if not item:
            continue
        try:
            value = item.decode("utf-8", "strict")
        except UnicodeError as error:
            raise ContextError(f"{repository.name} contains a non-UTF-8 path") from error
        paths.add(_safe_relative(value).as_posix())
    return paths


def _require_no_symlink_ancestors(root: Path, relative: PurePosixPath) -> None:
    current = root
    for part in relative.parts[:-1]:
        current = current / part
        try:
            value = current.lstat()
        except FileNotFoundError:
            return
        if stat.S_ISLNK(value.st_mode):
            raise ContextError(
                f"worktree path has a symlink ancestor: {relative.as_posix()}"
            )
        if not stat.S_ISDIR(value.st_mode):
            raise ContextError(
                f"worktree path has a non-directory ancestor: {relative.as_posix()}"
            )


def _overlay_worktree(
    repository: Repository,
    destination: Path,
    exported: set[str],
) -> set[str]:
    listed = _listed_worktree_paths(repository)
    deletions: set[str] = set()
    for raw in sorted(exported | listed):
        relative = _safe_relative(raw)
        _require_no_symlink_ancestors(repository.root, relative)
        source = repository.root.joinpath(*relative.parts)
        target = _destination(destination, relative)
        try:
            status_value = source.lstat()
        except FileNotFoundError:
            if raw in exported:
                _remove_path(target)
                deletions.add(raw)
            continue
        if stat.S_ISREG(status_value.st_mode):
            try:
                content = source.read_bytes()
            except OSError as error:
                raise ContextError(f"cannot read {repository.name}/{raw}") from error
            _write_regular(target, content, bool(status_value.st_mode & 0o111))
        elif stat.S_ISLNK(status_value.st_mode):
            try:
                link_target = os.readlink(source)
            except OSError as error:
                raise ContextError(f"cannot read symlink {repository.name}/{raw}") from error
            _write_symlink(target, relative, link_target)
        else:
            raise ContextError(f"unsupported worktree entry type: {repository.name}/{raw}")
    return deletions


def _entry_manifest(root: Path, deletions: dict[str, set[str]]) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for repository_name in sorted(deletions):
        for path in sorted(deletions[repository_name]):
            entries.append({
                "executable": False,
                "path": f"{repository_name}/{path}",
                "type": "deleted",
            })
    for path in sorted(root.rglob("*"), key=lambda value: value.as_posix()):
        relative = path.relative_to(root).as_posix()
        if relative == MANIFEST_NAME:
            continue
        value = path.lstat()
        if stat.S_ISDIR(value.st_mode):
            continue
        if stat.S_ISREG(value.st_mode):
            content = path.read_bytes()
            entries.append({
                "content_sha256": hashlib.sha256(content).hexdigest(),
                "executable": bool(value.st_mode & 0o111),
                "path": relative,
                "size": len(content),
                "type": "file",
            })
        elif stat.S_ISLNK(value.st_mode):
            entries.append({
                "executable": False,
                "path": relative,
                "target": os.readlink(path),
                "type": "symlink",
            })
        else:
            raise ContextError(f"unsupported staged context type: {relative}")
    return sorted(entries, key=lambda item: (str(item["path"]), str(item["type"])))


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode("ascii")


def prepare(options: argparse.Namespace) -> dict[str, object]:
    output = options.output.resolve(strict=True)
    if not output.is_dir() or any(output.iterdir()):
        raise ContextError("--output must be an existing empty directory")
    output.chmod(0o700)
    repositories = (
        _require_repository("strategy-service", options.service_repository),
        _require_repository("strategy-library", options.library_repository),
        _require_repository("golang-lib", options.golang_lib_repository),
        _require_repository("core-service", options.core_repository),
    )
    for repository in repositories:
        try:
            output.relative_to(repository.root)
        except ValueError:
            pass
        else:
            raise ContextError("--output must be outside every input repository")
    if any(repository.dirty for repository in repositories) and not options.allow_dirty:
        dirty_names = ", ".join(repository.name for repository in repositories if repository.dirty)
        raise ContextError(f"dirty runtime build input repositories: {dirty_names}")
    if len(options.profile_digest) != 64 or any(
        character not in "0123456789abcdef" for character in options.profile_digest
    ):
        raise ContextError("--profile-digest must be a lowercase SHA-256 value")

    deletions: dict[str, set[str]] = {}
    for repository in repositories:
        destination = output / repository.name
        destination.mkdir(mode=0o755)
        exported = _export_head(repository, destination)
        deletions[repository.name] = (
            _overlay_worktree(repository, destination, exported)
            if options.allow_dirty
            else set()
        )

    manifest = {
        "entries": _entry_manifest(output, deletions),
        "repositories": [
            {
                "commit": repository.commit,
                "dirty": repository.dirty,
                "name": repository.name,
            }
            for repository in repositories
        ],
        "schema_version": 1,
    }
    manifest_bytes = _canonical(manifest)
    (output / MANIFEST_NAME).write_bytes(manifest_bytes)
    state_input = _canonical(
        {
            "commits": {repository.name: repository.commit for repository in repositories},
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "profile_digest": options.profile_digest,
        }
    )
    return {
        "commits": {repository.name: repository.commit for repository in repositories},
        "source_dirty": any(repository.dirty for repository in repositories),
        "source_state_sha256": hashlib.sha256(state_input).hexdigest(),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prepare_runtime_build_context.py")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--service-repository", type=Path, required=True)
    parser.add_argument("--library-repository", type=Path, required=True)
    parser.add_argument("--golang-lib-repository", type=Path, required=True)
    parser.add_argument("--core-repository", type=Path, required=True)
    parser.add_argument("--profile-digest", required=True)
    parser.add_argument("--allow-dirty", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        payload = prepare(_parser().parse_args(argv))
    except (ContextError, OSError, UnicodeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    sys.stdout.buffer.write(_canonical(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
