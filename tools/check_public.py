#!/usr/bin/env python3
"""Fail when committed content or commit metadata names private projects, personal paths or emails.

Every blob reachable from any ref, every staged blob, and every commit's message, names and emails
are checked. Personal paths and email addresses are matched by built-in patterns. Private project
and person names are not built in, since listing them here would publish them: keep them in a
file outside the repository, one term per line (# starts a comment), and pass it with --terms or
WORKFLOW_AUDITOR_PRIVATE_TERMS. Findings name the location and the kind, never the matched text.
"""
from pathlib import Path
import argparse
import os
import re
import subprocess
import sys

# A home folder is personal unless its name is a placeholder.
HOME = re.compile(r"(?i)(?:/(?:home|users)/|\b[a-z]:[\\/]+users[\\/]+)([^/\\\s'\"`<>)\]]+)")
PLACEHOLDERS = {"user", "username", "you", "me", "name", "example", "runner", "...", "<user>", "$user"}
EMAIL = re.compile(r"[\w.%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")
ALLOWED_EMAIL = re.compile(r"(?i)^(?:[\w.+-]+@users\.noreply\.github\.com|noreply@anthropic\.com|"
                           r"[\w.+-]+@example\.(?:com|org|net))$")


def git(repo: Path, *argv: str, data: bytes | None = None) -> bytes:
    return subprocess.run(["git", "-C", str(repo), *argv], input=data, capture_output=True, check=True).stdout


def load_terms(path: Path) -> list[tuple[int, re.Pattern]]:
    terms = []
    for number, line in enumerate(path.read_text().splitlines(), 1):
        term = line.split("#", 1)[0].strip()
        if term:
            terms.append((number, re.compile(r"(?i)(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])")))
    return terms


def findings(text: str, terms: list[tuple[int, re.Pattern]]) -> list[tuple[int, str]]:
    found = []
    for line, content in enumerate(text.splitlines(), 1):
        if any(m.group(1).lower() not in PLACEHOLDERS for m in HOME.finditer(content)):
            found.append((line, "personal path"))
        if any(not ALLOWED_EMAIL.match(m.group(0)) for m in EMAIL.finditer(content)):
            found.append((line, "email address"))
        found.extend((line, f"private term (line {n} of the term list)") for n, term in terms if term.search(content))
    return found


def blobs(repo: Path) -> dict[str, str]:
    """Blob id to the first path it was seen under, across all refs and the index."""
    paths: dict[str, str] = {}
    listed = git(repo, "rev-list", "--objects", "--all").decode().splitlines()
    candidates = [line.split(" ", 1) for line in listed if " " in line]
    for line in git(repo, "ls-files", "-s").decode().splitlines():
        meta, path = line.split("\t", 1)
        candidates.append([meta.split()[1], path])
    if not candidates:
        return paths
    kinds = git(repo, "cat-file", "--batch-check=%(objectname) %(objecttype)",
                data="".join(sha + "\n" for sha, _ in candidates).encode()).decode().split()
    blob_ids = {kinds[i] for i in range(0, len(kinds), 2) if kinds[i + 1] == "blob"}
    for sha, path in candidates:
        if sha in blob_ids:
            paths.setdefault(sha, path)
    return paths


def check(repo: Path, terms: list[tuple[int, re.Pattern]]) -> list[str]:
    report = []
    for sha, path in sorted(blobs(repo).items(), key=lambda item: item[1]):
        text = git(repo, "cat-file", "blob", sha).decode("utf-8", "replace")
        report.extend(f"{path}:{line} (blob {sha[:12]}): {kind}" for line, kind in findings(text, terms))
    log = git(repo, "log", "--all", "--format=%H%x00%an%x00%ae%x00%cn%x00%ce%x00%B%x1e").decode()
    for record in filter(str.strip, log.split("\x1e")):
        commit, author, author_email, committer, committer_email, message = record.strip("\n").split("\x00", 5)
        for label, email in (("author", author_email), ("committer", committer_email)):
            if not ALLOWED_EMAIL.match(email):
                report.append(f"commit {commit[:12]}: {label} email address is not a noreply address")
        text = "\n".join((author, committer, message))
        report.extend(f"commit {commit[:12]} metadata line {line}: {kind}" for line, kind in findings(text, terms))
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--terms", type=Path, default=os.environ.get("WORKFLOW_AUDITOR_PRIVATE_TERMS") or None,
                        help="file of private terms kept outside the repository")
    args = parser.parse_args(argv)
    try:
        terms = load_terms(args.terms) if args.terms else []
        report = check(args.repo, terms)
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f"check_public: {exc}", file=sys.stderr)
        return 2
    if not args.terms:
        print("check_public: no private-term list given; only personal paths and emails were checked.", file=sys.stderr)
    for line in report:
        print(line)
    print(f"check_public: {len(report)} finding(s).")
    return 1 if report else 0


if __name__ == "__main__":
    raise SystemExit(main())
