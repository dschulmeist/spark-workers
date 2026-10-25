import hashlib
import os
import subprocess
import tempfile
from pathlib import Path


def git(repo, *args, env=None, input=None):
    process = subprocess.run(
        ["git", "-C", str(repo), *args], input=input, capture_output=True, check=False, env=env
    )
    if process.returncode:
        raise ValueError(process.stderr.decode(errors="replace").strip())
    return process.stdout


def validate_paths(paths):
    for value in paths:
        path = Path(value)
        if not value or path.is_absolute() or ".." in path.parts or ".git" in path.parts:
            raise ValueError("Paths must be repository-relative and must not contain '..' or '.git'")


def source_signature(repo):
    digest = hashlib.sha256(git(repo, "diff", "HEAD", "--binary"))
    digest.update(git(repo, "diff", "--cached", "--binary"))
    for name in git(repo, "ls-files", "--others", "--exclude-standard", "-z").decode().split("\0"):
        if name:
            path = repo / name
            digest.update(name.encode())
            digest.update(os.readlink(path).encode() if path.is_symlink() else path.read_bytes())
    return digest.hexdigest()


def tree_from_working_copy(repo, base, state_dir):
    with tempfile.TemporaryDirectory(prefix="index-", dir=state_dir) as temporary:
        env = {**os.environ, "GIT_INDEX_FILE": str(Path(temporary) / "index")}
        git(repo, "read-tree", base, env=env)
        git(repo, "add", "-A", "--", ".", env=env)
        return git(repo, "write-tree", env=env).decode().strip()


def prepare(repo, base_commit, include_working_tree, destination, state_dir):
    repo = Path(repo).resolve(strict=True)
    root = Path(git(repo, "rev-parse", "--show-toplevel").decode().strip()).resolve()
    if root != repo:
        raise ValueError("repository must be the Git repository root")
    if any(line.startswith(b"160000 ") for line in git(repo, "ls-files", "--stage").splitlines()):
        raise ValueError("Repositories with submodules need an explicit submodule snapshot strategy")
    base = git(repo, "rev-parse", "--verify", base_commit + "^{commit}").decode().strip()
    head = git(repo, "rev-parse", "HEAD").decode().strip()
    before = source_signature(repo)
    if include_working_tree:
        if base != head:
            raise ValueError("Including uncommitted files requires base_commit to equal the current HEAD")
        if git(repo, "ls-files", "--unmerged"):
            raise ValueError("Resolve merge conflicts before taking a worker snapshot")
        tree = tree_from_working_copy(repo, base, state_dir)
        snapshot = (
            git(repo, "commit-tree", tree, "-p", base, input=b"Worker input snapshot\n").decode().strip()
        )
    else:
        snapshot = base
    if source_signature(repo) != before or git(repo, "rev-parse", "HEAD").decode().strip() != head:
        raise ValueError("Source changed while the snapshot was being created; retry with a fresh base")
    git(repo, "worktree", "add", "--detach", str(destination), snapshot)
    return {
        "repository": str(repo),
        "base_commit": base,
        "snapshot_commit": snapshot,
        "source_signature": before,
        "worktree": str(destination),
    }


def within_scope(name, allowed):
    return any(
        value == "." or name == value.rstrip("/") or name.startswith(value.rstrip("/") + "/")
        for value in allowed
    )


def export_changes(job, artifact_dir):
    worktree = Path(job["workspace"]["worktree"])
    baseline = job["workspace"]["snapshot_commit"]
    tree = tree_from_working_copy(worktree, baseline, artifact_dir)
    names = git(worktree, "diff", "--name-only", "-z", baseline, tree).decode().split("\0")
    names = [name for name in names if name]
    patch = git(worktree, "diff", "--binary", "--full-index", baseline, tree)
    (artifact_dir / "changes.patch").write_bytes(patch)
    violations = [name for name in names if not within_scope(name, job["allowed_paths"])]
    return {
        "changed_files": names,
        "scope_violations": violations,
        "patch": str(artifact_dir / "changes.patch"),
        "result_tree": tree,
    }


def permissions(worktree, allowed_paths):
    root = str(worktree)
    rules = [{"action": "*", "resource": "*", "effect": "deny"}]
    for action in ["read", "glob", "grep"]:
        rules.append({"action": action, "resource": "*", "effect": "allow"})
    for path in allowed_paths:
        absolute = str(Path(root) / path)
        for resource in [path, path.rstrip("/") + "/**", absolute, absolute.rstrip("/") + "/**"]:
            rules.append({"action": "edit", "resource": resource, "effect": "allow"})
    rules.append({"action": "shell", "resource": "*", "effect": "ask"})
    return rules
