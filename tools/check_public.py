#!/usr/bin/env python3
"""Fail when a repository's content, path names, refs or metadata name private projects, personal paths or emails.

Checked: every blob reachable from any ref and every staged blob; every path name in a commit's
tree, in a tree that a ref or an annotated tag points to, and in the index; every ref name; and
every commit's and annotated tag's headers and message, including a merged tag. Author, committer
and tagger emails must be noreply addresses. Blobs are read as UTF-8 text with undecodable bytes
replaced. Replace refs are ignored, since a push sends the original objects.

Personal paths and email addresses are matched by built-in patterns. Private project and person
names are not built in, since listing them here would publish them: keep them in a file outside
the repository, one term per line (# starts a comment), and pass it with --terms or
WORKFLOW_AUDITOR_PRIVATE_TERMS. A term's parts, split at non-alphanumerics and at lower-to-upper
case changes, match in any case joined by their own separator, -, _, ., whitespace or nothing. A
term also matches at a CamelCase join or before a digit, but not inside a longer lowercase word.
Where no list is available, for example in a public CI job, pass --no-terms; the check then says
that names were not checked. Findings name the location and the kind, never the matched text; a
path or ref name with a finding is shown by object id instead.

Exit status: 0 clean, 1 findings, 2 could not check (no list and no --no-terms, a list that is
inside the repository, unreadable, not UTF-8 or without terms, --repo that is not the top level
of a repository, or a Git error).
"""
from pathlib import Path
import argparse
import os
import re
import subprocess
import sys

# A home folder is personal unless its name is a placeholder. Each pattern captures the folder's
# name, except the root account's home, which is always personal.
SEP = r"(?:[\\/]|%2f|%5c|\\u002f|\\u005c)"  # a slash or backslash, also URL- or JSON-escaped
HOMES = (
    re.compile(rf"(?i){SEP}+(?:home|users){SEP}+([^/\\\s'\"`<>)\]%]+)"),  # Unix, macOS, Windows, WSL, UNC
    re.compile(r"(?i)(?<![\w-])(?:-home|-users|-mnt-[a-z]-users|[a-z]--users)-([^-/\\\s'\"`<>)\]%]+)"),  # Claude Code project folders
    re.compile(r"(?i)(?<![\w/~])~([a-z_][\w.-]*)/"),
    re.compile(r"(?<![\w.~])/(?:root)/"),  # grouped so the pattern does not match its own source
)
PLACEHOLDERS = {"user", "username", "you", "me", "name", "example", "runner", "...", "<user>", "$user"}
EMAIL = re.compile(r"[\w.%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")
ALLOWED_EMAIL = re.compile(r"(?i)^(?:[\w.+-]+@users\.noreply\.github\.com|noreply@anthropic\.com|"
                           r"[\w.+-]+@example\.(?:com|org|net))$")
JOIN = r"[-_.\s]*"
IDENTITY = re.compile(r"^ ?(author|committer|tagger) .*?<([^<>]*)>")  # also indented, inside a merged tag
SIGNATURES = {"gpgsig", "gpgsig-sha256"}
IDS_ONLY = {"tree", "parent", "object", "type"}
# Variables that point Git away from --repo. GIT_INDEX_FILE is not one: a pre-commit hook sets it to the index being committed.
REDIRECTS = {"GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES"}


class CannotCheck(Exception):
    """The check could not run, which is not the same as finding nothing."""


def git(repo: Path, *argv: str, data: bytes | None = None) -> bytes:
    env = {**{k: v for k, v in os.environ.items() if k not in REDIRECTS},
           "GIT_NO_REPLACE_OBJECTS": "1"}  # a push sends the original objects, not their replacements
    try:
        return subprocess.run(["git", "-C", str(repo), *argv], input=data, capture_output=True, check=True,
                              env=env).stdout
    except subprocess.CalledProcessError as exc:  # fixed text: the command line holds the repository path
        raise CannotCheck(f"git {argv[0]} failed (exit {exc.returncode})") from None
    except OSError as exc:
        raise CannotCheck(f"cannot run git: {exc.strerror}") from None


def text(raw: bytes, codec: str = "utf-8") -> str:
    try:
        return raw.decode(codec, "replace")
    except (LookupError, ValueError):  # unknown codec, one that refuses every input, or a name with a NUL
        return raw.decode("utf-8", "replace")


def lines(raw: bytes) -> list[str]:
    """Output lines, split at newlines only: a name may hold other line boundaries."""
    return [line for line in text(raw).split("\n") if line]


def term_pattern(term: str) -> re.Pattern | None:
    spaced = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", term)
    parts = re.findall(r"[^\W_]+", spaced)
    if not parts:
        return None
    gaps = re.findall(r"(?<=[^\W_])[\W_]+(?=[^\W_])", spaced)
    body = re.escape(parts[0]) + "".join(f"(?:{re.escape(gap)}|{JOIN}){re.escape(part)}" for gap, part in zip(gaps, parts[1:]))
    # Case folding applies to the term only: an uppercase letter or a digit may follow it, and it
    # may follow a lowercase letter or digit only where it starts with an uppercase letter.
    end = r"(?![a-z0-9])" if parts[-1][-1].isdigit() else r"(?![a-z])"
    return re.compile(r"(?:(?<![a-z0-9])|(?=[A-Z]))(?i:" + body + ")" + end)


def load_terms(path: Path) -> list[tuple[int, re.Pattern]]:
    terms = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        pattern = term_pattern(line.split("#", 1)[0].strip())
        if pattern:
            terms.append((number, pattern))
    return terms


def personal(match: re.Match) -> bool:
    return not match.groups() or match.group(1).lower() not in PLACEHOLDERS


def findings(text: str, terms: list[tuple[int, re.Pattern]]) -> list[tuple[int, str]]:
    found = []
    for line, content in enumerate(text.splitlines(), 1):
        if any(personal(m) for pattern in HOMES for m in pattern.finditer(content)):
            found.append((line, "personal path"))
        if any(not ALLOWED_EMAIL.match(m.group(0)) for m in EMAIL.finditer(content)):
            found.append((line, "email address"))
        found.extend((line, f"private term (line {n} of the term list)") for n, term in terms if term.search(content))
    return found


def read(repo: Path, ids: list[str]) -> dict[str, bytes]:
    """Raw content of each object, from one cat-file --batch."""
    if not ids:
        return {}
    out = git(repo, "cat-file", "--batch", data="".join(sha + "\n" for sha in ids).encode())
    objects, pos = {}, 0
    while pos < len(out):
        end = out.index(b"\n", pos)
        sha, _, size = out[pos:end].decode().split()
        objects[sha] = out[end + 1:end + 1 + int(size)]
        pos = end + 1 + int(size) + 1
    return objects


def codecs(raw: bytes) -> list[str]:
    """UTF-8, which is how Git parses the identity headers whatever the encoding header says, and the declared encoding."""
    declared = re.search(rb"(?m)^encoding (\S+)", raw.partition(b"\n\n")[0])
    return list(dict.fromkeys(["utf-8", declared.group(1).decode("ascii", "replace") if declared else "utf-8"]))


def headers(raw: bytes, codec: str) -> tuple[list[str], str]:
    """A commit's or tag's header lines that can hold a name, without signatures, and its message."""
    head, _, body = raw.partition(b"\n\n")
    kept, signed = [], False
    for line in text(head, codec).split("\n"):
        if not line.startswith(" "):
            signed = line.split(" ", 1)[0] in SIGNATURES
        if not signed and line.split(" ", 1)[0] not in IDS_ONLY:
            kept.append(line)
    return kept, text(body, codec)


def metadata(kind: str, sha: str, raw: bytes, terms: list[tuple[int, re.Pattern]]) -> list[str]:
    """Findings in a commit or tag object: identities must be noreply addresses; the rest is scanned."""
    report = []
    for codec in codecs(raw):  # the message is in the declared encoding; the headers may be mislabelled
        shown = []
        head, message = headers(raw, codec)
        for line in head:
            who = IDENTITY.match(line)
            if who:
                if not ALLOWED_EMAIL.match(who.group(2)):
                    report.append(f"{kind} {sha[:12]}: {who.group(1)} email address is not a noreply address")
                    line = line[:who.start(2)] + line[who.end(2):]  # reported above; an allowed one is still scanned for terms
            shown.append(line)
        content = "\n".join([*shown, message])
        report.extend(f"{kind} {sha[:12]} metadata line {line}: {problem}" for line, problem in findings(content, terms))
    return list(dict.fromkeys(report))


def index(repo: Path) -> list[tuple[str, str, str]]:
    """(mode, object id, path) for every staged entry."""
    staged = []
    for entry in filter(None, git(repo, "ls-files", "-s", "-z").split(b"\0")):
        meta, path = entry.split(b"\t", 1)
        mode, sha, _ = meta.decode().split()
        staged.append((mode, sha, text(path)))
    return staged


def entries(repo: Path, roots: dict[str, str], staged: list[tuple[str, str, str]]) -> list[tuple[str, str, str, str]]:
    """(object id, type, path, where) for every path in the given root trees and in the index."""
    found = []
    for tree, where in roots.items():
        for entry in filter(None, git(repo, "ls-tree", "-r", "-t", "-z", tree).split(b"\0")):
            meta, path = entry.split(b"\t", 1)
            _, kind, sha = meta.decode().split()
            found.append((sha, kind, text(path), where))
    found.extend((sha, "commit" if mode == "160000" else "blob", path, "staged") for mode, sha, path in staged)
    return found


def check(repo: Path, terms: list[tuple[int, re.Pattern]]) -> list[str]:
    report = []
    refs = [line.split(" ", 2) for line in lines(git(repo, "for-each-ref", "--format=%(objectname) %(objecttype) %(refname)"))]
    staged = index(repo)
    listed = [line.split(" ", 1)[0] for line in lines(git(repo, "rev-list", "--objects", "--all"))]
    ids = sorted({*listed, *(sha for mode, sha, _ in staged if mode != "160000")})
    types = dict(line.split(" ") for line in lines(git(repo, "cat-file", "--batch-check=%(objectname) %(objecttype)",
                                                      data="".join(sha + "\n" for sha in ids).encode()))) if ids else {}
    objects = read(repo, [sha for sha in ids if types[sha] in ("commit", "tag")])
    # Root trees come from commits, from tags that point to a tree, and from refs that do.
    roots: dict[str, str] = {sha: "ref" for sha, kind, _ in refs if kind == "tree"}
    for sha, raw in objects.items():
        found = re.match(rb"tree ([0-9a-f]+)\n", raw) if types[sha] == "commit" else re.match(rb"object ([0-9a-f]+)\ntype tree\n", raw)
        if found:
            roots.setdefault(found.group(1).decode(), f"{types[sha]} {sha[:12]}")
    named = entries(repo, roots, staged)
    flagged = set()
    for sha, kind, path, where in named:
        if path not in flagged and (found := findings("/" + "/".join(path.splitlines()), terms)):
            flagged.add(path)
            report.extend(f"path of {kind} {sha[:12]} ({where}): {problem}" for _, problem in found)
    paths: dict[str, list[str]] = {}
    for sha, _, path, _ in named:
        paths.setdefault(sha, []).append(path)
    labels = {sha: next((p for p in paths.get(sha, []) if p not in flagged), "(path withheld)" if sha in paths else "(no path)")
              for sha, kind in types.items() if kind == "blob"}
    for sha, label in sorted(labels.items(), key=lambda item: item[1]):
        content = text(git(repo, "cat-file", "blob", sha))
        report.extend(f"{label}:{line} (blob {sha[:12]}): {kind}" for line, kind in findings(content, terms))
    for sha, raw in sorted(objects.items()):
        report.extend(metadata(types[sha], sha, raw, terms))
    for number, (sha, _, name) in enumerate(refs, 1):
        report.extend(f"ref {number} (at {sha[:12]}): {kind}" for _, kind in findings(name, terms))
    return report


def inside(path: Path, repo: Path) -> bool:
    """Whether path lies in repo, as written or with symbolic links resolved."""
    places = {Path(os.path.abspath(repo)), repo.resolve()}
    return any(p == r or r in p.parents for p in (Path(os.path.abspath(path)), path.resolve()) for r in places)


def toplevel(repo: Path) -> Path:
    """The folder of the repository that --repo belongs to, or its Git directory when it is bare."""
    bare = text(git(repo, "rev-parse", "--is-bare-repository")).strip() == "true"
    top = git(repo, "rev-parse", "--absolute-git-dir" if bare else "--show-toplevel").rstrip(b"\n")
    try:
        same = os.path.samefile(repo, os.fsdecode(top))
    except OSError:
        same = False
    if not same:
        raise CannotCheck("--repo must be the top-level folder of a repository, or a bare repository")
    return Path(os.fsdecode(top))


def prepare(args: argparse.Namespace) -> tuple[Path, list[tuple[int, re.Pattern]]]:
    """The repository to check and its terms, or CannotCheck."""
    if args.no_terms and args.terms:
        raise CannotCheck("--no-terms contradicts the term list given")
    if not args.no_terms and not args.terms:
        raise CannotCheck("no private-term list; pass --terms FILE, set WORKFLOW_AUDITOR_PRIVATE_TERMS, or pass --no-terms")
    top = toplevel(args.repo)
    if not args.terms:
        return top, []
    if inside(args.terms, top):
        raise CannotCheck("the term list is inside the repository; keep it outside so it is never committed")
    try:
        terms = load_terms(args.terms)
    except UnicodeError:
        raise CannotCheck("the term list is not UTF-8 text") from None
    except OSError as exc:
        raise CannotCheck(f"cannot read the term list: {exc.strerror}") from None
    if not terms:
        raise CannotCheck("the term list holds no terms")
    return top, terms


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0],
                                     epilog="Exit status: 0 clean, 1 findings, 2 could not check.")
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--terms", type=Path, default=os.environ.get("WORKFLOW_AUDITOR_PRIVATE_TERMS") or None,
                        help="file of private terms kept outside the repository")
    parser.add_argument("--no-terms", action="store_true",
                        help="check without a term list, where none is available")
    args = parser.parse_args(argv)
    try:
        top, terms = prepare(args)
        report = check(top, terms)
    except CannotCheck as exc:
        print(f"check_public: {exc}.", file=sys.stderr)
        return 2
    if args.no_terms:
        print("check_public: --no-terms: private names were not checked; only personal paths and emails were.",
              file=sys.stderr)
    for line in report:
        print(line)
    print(f"check_public: {len(report)} finding(s).")
    return 1 if report else 0


if __name__ == "__main__":
    raise SystemExit(main())
