from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator
import hashlib
import json
import re

from .model import Event, Tokens, identifier, money, number, optional_number, timestamp
from .store import Store
from .retention import capture, captured_file, parent_directory_present, input_shape, result_shape, root_states, membership, membership_settings

# Other agent types map to roles only through config "type_roles".
ROLES = {"reviewer": "reviewer", "implementer": "implementer", "scout": "scout"}
# Errors from unexpected record shapes. They are recorded per row or per file; anything else
# (database errors, defects) escapes and leaves the import visibly "running".
SHAPE_ERRORS = (ValueError, TypeError, KeyError, AttributeError, IndexError)
CODEX_COUNTS = ("input_tokens", "cached_input_tokens", "cache_write_input_tokens",
                "output_tokens", "reasoning_output_tokens")


def path_key(value: str) -> str:
    value = value.replace("\\", "/").rstrip("/")
    if re.match(r"^[a-zA-Z]:/", value):
        value = "/mnt/" + value[0].lower() + value[2:]
    return value.casefold() if re.match(r"^/mnt/[a-z]/", value) else value


def local_path(value: str) -> Path:
    value = value.replace("\\", "/")
    if re.match(r"^[a-zA-Z]:/", value):
        value = "/mnt/" + value[0].lower() + value[2:]
    return Path(value).expanduser()


def repo_name(cwd: str | None, source: dict, config: dict, fallback: str | None = None) -> str:
    if source.get("repo"):
        return source["repo"]
    if cwd:
        key = path_key(cwd)
        aliases = sorted(config.get("repo_aliases", {}).items(), key=lambda p: -len(path_key(p[0])))
        for path, repo in aliases:
            prefix = path_key(path)
            if key == prefix or key.startswith(prefix + "/"):
                return repo
    return repo_fallback(cwd) if fallback is None else fallback


def repo_fallback(cwd: str | None) -> str:
    # Discovery labels are paths; no speculative decoding of .claude directory names.
    # Capture this even when an alias wins, so removing it can be replayed without a live cwd.
    if cwd:
        candidate = local_path(cwd)
        if candidate.is_dir():
            for parent in (candidate, *candidate.parents):
                if (parent / ".git").exists():
                    return path_key(str(parent))
    return "unattributed"


def provider(model: str, source: dict, recorded: str | None = None) -> tuple[str, str]:
    if source.get("provider"):
        return source["provider"], "registration"
    if recorded:
        return {"zai-coding": "z.ai", "zai": "z.ai"}.get(recorded, recorded), "client_recorded"
    if model.startswith("claude-"):
        return "anthropic", "model_family_inference"
    if "glm-" in model:
        return "z.ai", "model_family_inference"
    if model.startswith("gpt-"):
        return "openai", "model_family_inference"
    return "unknown", "unknown"


def role_name(agent: str | None, agent_type: str | None, root: bool, source: dict, config: dict) -> tuple[str, str]:
    overrides = config.get("agent_roles", {})
    if agent in overrides:
        return overrides[agent], "explicit_agent_map"
    if source.get("role"):
        return source["role"], "registration"
    types = {**ROLES, **config.get("type_roles", {})}
    if agent_type in types:
        return types[agent_type], "agent_type"
    return ("lead", "structural_root") if root else ("unknown", "unclassified")


def replay_attribution(inputs: dict, source: dict, config: dict) -> dict:
    if inputs["explicit"] is not None:
        return dict(inputs["explicit"])
    agent = (source.get("agent") or inputs["native_agent"]) if source["kind"] == "router" else inputs["agent"]
    prov, basis = provider(inputs["model"], source, inputs["recorded_provider"])
    role, role_basis = role_name(agent, inputs["agent_type"], inputs["root"], source, config)
    return {"repo": repo_name(inputs["cwd"], source, config, inputs["fallback"]), "provider": prov,
            "provider_basis": basis, "agent": agent, "role": role, "role_basis": role_basis,
            "included": not (source["kind"] == "claude" and inputs["native_agent"] in source.get("exclude_agents", []))}


class Attribution:
    def __init__(self, source: dict, config: dict):
        self.source, self.config, self.pairs = source, config, {}
        self.replayable = True

    def of(self, *, event=None, cwd=None, model="unknown", recorded=None, native=None,
           agent=None, agent_type=None, root=False, explicit=False):
        # Capture observes main's result; malformed metadata must never change the parser path.
        try:
            if (cwd and not isinstance(cwd, str)) or (recorded and not isinstance(recorded, str)):
                self.replayable = False
            inputs = dict(cwd=cwd if isinstance(cwd, str) else None, fallback="unattributed", model=model,
                          recorded_provider=recorded if isinstance(recorded, str) else None, native_agent=native,
                          agent=agent, agent_type=agent_type if isinstance(agent_type, str) else None,
                          root=root, explicit=None)
            if not input_shape(inputs):
                self.replayable = False
                return
            inputs["fallback"] = repo_fallback(inputs["cwd"])
            result = ({**{k: getattr(event, k) for k in
                       ("repo", "provider", "provider_basis", "agent", "role", "role_basis")}, "included": True}
                      if event is not None else replay_attribution(inputs, self.source, self.config))
            if explicit:
                inputs["explicit"] = result
            if not result_shape(result):
                self.replayable = False
                return
            self.pairs[json.dumps(inputs, sort_keys=True)] = {"inputs": inputs, "result": result}
        except (OSError, *SHAPE_ERRORS):
            self.replayable = False


def claude_tokens(usage: dict) -> Tokens:
    flat = ("input_tokens", "output_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")
    if not any(usage.get(k, 0) for k in flat) and usage.get("iterations"):
        parts = [claude_tokens(item) for item in usage["iterations"]]
        def add(field: str) -> int | None:
            values = [getattr(x, field) for x in parts]
            return sum(values) if all(v is not None for v in values) else None
        return Tokens(**{k: add(k) for k in Tokens.__dataclass_fields__})
    fresh, output = optional_number(usage.get("input_tokens")), optional_number(usage.get("output_tokens"))
    read = number(usage.get("cache_read_input_tokens", 0))
    detail = usage.get("cache_creation") or {}
    w5 = optional_number(detail.get("ephemeral_5m_input_tokens"))
    w1 = optional_number(detail.get("ephemeral_1h_input_tokens"))
    written = number(usage.get("cache_creation_input_tokens", (w5 or 0) + (w1 or 0)))
    inp = fresh + read + written if fresh is not None else None
    reasoning = optional_number((usage.get("output_tokens_details") or {}).get("thinking_tokens"))
    return Tokens(inp, read, written, output, reasoning, w5, w1)


def service_option(value: object, standard: str) -> str:
    # Claude's speed and inference_geo: a missing key or null is unknown, an explicit "" is the
    # standard value (standard, global), and anything else is kept as recorded.
    return "unknown" if value is None else standard if value == "" else str(value)


def codex_tokens(usage: dict) -> Tokens:
    return Tokens(*(optional_number(usage.get(k)) for k in CODEX_COUNTS))


def json_object(raw: str | bytes) -> dict:
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("JSON document must be an object")
    return value


@dataclass
class Parsed:
    events: list[Event]
    issues: list[dict]
    digest: str
    raw_lines: int
    receipt: dict


def read_jsonl(path: Path, issues: list[dict], digest: object) -> Iterator[tuple[int, dict]]:
    with path.open("rb") as stream:
        for line, raw in enumerate(stream, 1):
            digest.update(raw)
            try:
                value = json.loads(raw)
                if isinstance(value, dict):
                    yield line, value
                else:
                    issues.append({"code": "json_record_not_object", "line": line})
            except (ValueError, UnicodeError):
                issues.append({"code": "invalid_json", "line": line})


def parse(path: Path, source: dict, config: dict) -> Parsed:
    kind = source["kind"]
    attribution = Attribution(source, config)
    def finish(events, issues, digest, lines):
        receipt = capture(source, digest, list(attribution.pairs.values()), attribution.replayable)
        return Parsed(events, issues, digest, lines, receipt)
    issues: list[dict] = []
    events: list[Event] = []
    digest = hashlib.sha256()
    last_line = 0
    meta: dict = {}
    previous: dict | None = None
    model = "unknown"
    last_result: dict | None = None
    result_cost: str | None = None
    settings: dict = {}
    if kind == "claude":
        sidecar = path.with_suffix(".meta.json")
        if sidecar.is_file():
            data = json_object(sidecar.read_text())
            settings = {k: data.get(k) for k in ("agentType", "parentAgentId")}
    if kind == "glm_report":
        raw = path.read_bytes()
        digest.update(raw)
        row = json_object(raw)
        if not row.get("session_id") or not isinstance(row.get("usage"), dict):
            return finish([], [{"code": "not_an_attempt_report"}], digest.hexdigest(), 1)
        model = str(row.get("model") or "unknown")
        prov, basis = provider(model, source)
        sid = str(row["session_id"])
        start, end = timestamp(row.get("started_utc")), timestamp(row.get("finished_utc"))
        role, role_basis = role_name(None, None, False, source, config)
        attribution.of(cwd=row.get("worktree"), model=model)
        events.append(Event(identifier("glm_report", sid, row.get("started_utc")), "claude_code", prov, basis,
                            model, repo_name(row.get("worktree"), source, config), sid, None, None, role, role_basis,
                            claude_tokens(row["usage"]), "aggregate", start=start, end=end,
                            cli_usd=money(row.get("total_cost_usd")), unit="attempt_total"))
        return finish(events, issues, digest.hexdigest(), 1)
    for line, row in read_jsonl(path, issues, digest):
        last_line = line
        try:
            if kind == "normalized":
                values = dict(row)
                for k in ("at", "start", "end", "activity_start", "activity_end"):
                    if isinstance(values.get(k), str):
                        values[k] = timestamp(values[k])
                events.append(Event.from_data(values))
                attribution.of(event=events[-1], model=events[-1].model, explicit=True)
            elif kind == "claude" and row.get("type") == "assistant" and isinstance(row.get("message"), dict):
                msg = row["message"]
                if not isinstance(msg.get("usage"), dict):
                    continue
                if row.get("session_id") and not row.get("timestamp"):
                    issues.append({"code": "stream_requires_claude_stream_adapter", "line": line})
                    continue
                fallback_sid = path.parent.parent.name if path.stem.startswith("agent-") else path.stem
                sid = str(row.get("sessionId") or fallback_sid)
                aid = row.get("agentId") or (path.stem.removeprefix("agent-") if path.stem.startswith("agent-") else "main")
                if aid in source.get("exclude_agents", []):
                    attribution.of(cwd=row.get("cwd"), model=str(msg.get("model") or "unknown"),
                                   native=aid, agent=f"claude:{sid}:{aid}",
                                   agent_type=settings.get("agentType"), root=aid == "main")
                    continue
                agent = f"claude:{sid}:{aid}"
                role, role_basis = role_name(agent, settings.get("agentType"), aid == "main", source, config)
                model = str(msg.get("model") or "unknown")
                prov, basis = provider(model, source)
                attribution.of(cwd=row.get("cwd"), model=model, native=aid,
                               agent=agent, agent_type=settings.get("agentType"), root=aid == "main")
                usage = msg["usage"]
                if not (msg.get("id") or row.get("requestId")):
                    # Progressive blocks of one response share no other key; a per-row uuid would count each block.
                    issues.append({"code": "missing_response_identity", "line": line})
                    continue
                key = identifier("claude_response", msg.get("id") or row.get("requestId"))
                parent = f"claude:{sid}:main" if aid != "main" else None
                if settings.get("parentAgentId"):
                    parent = f"claude:{sid}:{settings['parentAgentId']}"
                events.append(Event(key, "claude_code", prov, basis, model,
                                    repo_name(row.get("cwd"), source, config), sid, agent, parent, role, role_basis,
                                    claude_tokens(usage), "point", at=timestamp(row.get("timestamp")),
                                    speed=service_option(usage.get("speed"), "standard"),
                                    geo=service_option(usage.get("inference_geo"), "global")))
            elif kind == "codex":
                payload = row.get("payload") or {}
                if row.get("type") == "session_meta" and not meta:
                    meta = payload
                if row.get("type") == "turn_context" and payload.get("model"):
                    model = str(payload["model"])
                if row.get("type") != "event_msg" or payload.get("type") != "token_count":
                    continue
                current = (payload.get("info") or {}).get("total_token_usage")
                if not isinstance(current, dict) or not meta.get("id"):
                    issues.append({"code": "missing_counter_identity", "line": line})
                    continue
                vector = {k: optional_number(current.get(k)) for k in CODEX_COUNTS}
                if previous == vector:
                    continue
                if previous is not None and any(a is not None and b is not None and b < a for a, b in zip(previous.values(), vector.values())):
                    issues.append({"code": "counter_reset_unresolved", "line": line})
                    previous = vector
                    continue
                delta = {k: v - ((previous or {}).get(k) or 0) if v is not None and (previous is None or previous[k] is not None) else None
                         for k, v in vector.items()}
                previous = vector
                sid = str(meta["id"])
                parent_source = meta.get("source")
                parent_id = parent_source.get("subagent", {}).get("thread_spawn", {}).get("parent_thread_id") if isinstance(parent_source, dict) else None
                agent = f"codex:{sid}"
                role, role_basis = role_name(agent, None, not parent_id, source, config)
                prov, basis = provider(model, source, meta.get("model_provider"))
                attribution.of(cwd=meta.get("cwd"), model=model,
                               recorded=meta.get("model_provider"), native=sid, agent=agent, root=not parent_id)
                events.append(Event(identifier("codex_counter", sid, vector), "codex", prov, basis, model,
                                    repo_name(meta.get("cwd"), source, config), sid, agent,
                                    f"codex:{parent_id}" if parent_id else None, role, role_basis,
                                    codex_tokens(delta), "point", at=timestamp(row.get("timestamp")), unit="counter_delta"))
            elif kind == "claude_stream":
                if row.get("type") == "system" and row.get("subtype") == "init" and not meta:
                    meta = {k: row.get(k) for k in ("session_id", "cwd", "model")}
                if row.get("type") == "result":
                    last_result = row
                    value = money(row.get("total_cost_usd"))
                    if value is not None and (result_cost is None or float(value) > float(result_cost)):
                        result_cost = value
            elif kind == "router":
                if not row.get("requestId"):
                    issues.append({"code": "router_request_id_missing", "line": line})
                    continue
                model = str(row.get("model") or "unknown")
                prov, basis = provider(model, source, row.get("provider"))
                # Billed counters take precedence over local accepted-context estimates.
                tokens = Tokens(optional_number(row.get("billedInputTokens", row.get("inputTokens"))),
                                optional_number(row.get("cachedInputTokens")), 0,
                                optional_number(row.get("billedOutputTokens", row.get("outputTokens"))),
                                optional_number(row.get("reasoningTokens")))
                agent = source.get("agent") or row.get("agentId") or row.get("threadId")
                role, role_basis = role_name(agent, None, False, source, config)
                attribution.of(cwd=row.get("cwd"), model=model,
                               recorded=row.get("provider"), native=row.get("agentId") or row.get("threadId"))
                events.append(Event(identifier("router_request", prov, row["requestId"]), "model_router", prov, basis,
                                    model, repo_name(row.get("cwd"), source, config), str(row.get("threadId") or row["requestId"]),
                                    agent, None, role, role_basis, tokens, "point", at=timestamp(row.get("at")), unit="router_request"))
        except SHAPE_ERRORS as exc:
            issues.append({"code": "invalid_accounting_record", "line": line, "exception": type(exc).__name__})
    if kind == "claude_stream" and last_result:
        sid = str(last_result.get("session_id") or meta.get("session_id") or identifier(str(path)))
        models = last_result.get("modelUsage") or {}
        if not isinstance(models, dict) or not models:
            issues.append({"code": "final_model_usage_missing"})
            return finish(events, issues, digest.hexdigest(), last_line)
        for model, usage in models.items():
            prov, basis = provider(model, source)
            tokens = Tokens(number(usage.get("inputTokens", 0)) + number(usage.get("cacheReadInputTokens", 0)) + number(usage.get("cacheCreationInputTokens", 0)),
                            number(usage.get("cacheReadInputTokens", 0)), number(usage.get("cacheCreationInputTokens", 0)),
                            optional_number(usage.get("outputTokens")), optional_number(usage.get("thinkingTokens")))
            role, role_basis = role_name(None, None, False, source, config)
            attribution.of(cwd=meta.get("cwd"), model=model)
            events.append(Event(identifier("cli_total", sid, model), "claude_code", prov, basis, model,
                                repo_name(meta.get("cwd"), source, config), sid, None, None, role, role_basis,
                                tokens, "aggregate", start=timestamp(source.get("since")), end=timestamp(source.get("until")),
                                cli_usd=money(usage.get("costUSD")) if len(models) > 1 else result_cost, unit="run_model_total"))
    elif kind == "claude_stream":
        issues.append({"code": "final_result_missing"})
    return finish(events, issues, digest.hexdigest(), last_line)


def discover(source: dict) -> tuple[list[Path], list[dict], dict]:
    found, provenance = set(), {}
    roots = root_states(source)
    pattern = source.get("pattern") or ("**/report.json" if source["kind"] == "glm_report" else "**/*.jsonl")
    for value, root in zip(source.get("roots", []), roots):
        if root["health"] != "healthy":
            continue
        try:
            base = local_path(value)
            if root["type"] == "directory":
                list(base.iterdir())  # glob may swallow enumeration errors; check the root directly.
            paths = [base] if root["type"] == "file" else base.glob(pattern)
            for path in paths:
                if path.is_file():
                    resolved = path.resolve()
                    matched, _ = membership(str(resolved), source, [root],
                                            [{**root, "relative": path.relative_to(base).as_posix() if path != base else ""}], discovered=True)
                    if matched:
                        found.add(resolved)
                        provenance.setdefault(str(resolved), []).extend(matched)
        except (OSError, ValueError):
            root.update(health="error", type="unknown")
            for matches in provenance.values():
                for record in matches:
                    if record["identity"] == root["identity"]:
                        record.update(health="error", type="unknown")
    return sorted(found), roots, provenance


def validate_config(config: dict) -> None:
    """Checked before any database is created or opened for writing."""
    if not isinstance(config.get("sources"), list) or not config["sources"]:
        raise ValueError("Config needs a nonempty sources list")
    if not all(isinstance(s, dict) and isinstance(s.get("name"), str) and s["name"] for s in config["sources"]):
        raise ValueError("Each source registration needs a name")
    names = [s["name"] for s in config["sources"]]
    if len(names) != len(set(names)):
        raise ValueError("Source registration names must be unique")
    allowed = {"claude", "codex", "glm_report", "claude_stream", "router", "normalized"}
    for source in config["sources"]:
        if source.get("kind") not in allowed:
            raise ValueError("Unsupported source kind")
        if source.get("accounting_role", "primary") not in ("primary", "corroborating"):
            raise ValueError("Accounting role must be primary or corroborating")
        if source.get("rotation_policy", "retain") not in ("retain", "require_present"):
            raise ValueError("Rotation policy must be retain or require_present")
        pattern = source.get("pattern")
        if pattern is not None and (not isinstance(pattern, str) or not pattern or Path(pattern).is_absolute() or "\x00" in pattern
                        or any("**" in p and p != "**" for p in Path(pattern).parts)):
            raise ValueError("Pattern must be a usable relative glob")


def ingest(store: Store, config: dict) -> dict:
    validate_config(config)
    store.put_config(config, {"state": "running", "registered_sources": len(config["sources"])})
    imported, failed = 0, 0
    for source in config["sources"]:
        primary = source.get("accounting_role", "corroborating" if source["kind"] == "router" else "primary") == "primary"
        store.replay_receipts(source, config)
        try:
            paths, roots, provenance = discover(source)
        except OSError:
            paths, roots, provenance = [], root_states(source), {}
            for root in roots:
                root.update(health="error", type="unknown")
        missing = [local_path(value) for value, root in zip(source.get("roots", []), roots) if root["health"] != "healthy"]
        known = {s["id"]: s for s in store.sources(include_retired=True)}
        seen: set[str] = set()
        failed_imports: set[str] = set()
        for path in missing:
            path = str(path)
            missing_id = identifier(source["name"], path)
            root = roots[[str(local_path(v)) for v in source.get("roots", [])].index(path)]
            if (root["health"] == "missing" and source.get("rotation_policy", "retain") == "retain"
                    and parent_directory_present(root)
                    and any(s["registration"] == source["name"] and captured_file(root, s) for s in known.values())):
                continue
            if missing_id in known and known[missing_id]["sha256"] is not None:
                failed += 1
                continue
            seen.add(missing_id)
            code = "registered_root_missing" if root["health"] == "missing" else "registered_root_unreadable"
            store.failed_source(missing_id, source["name"], path, code, primary)
            failed += 1
        if not paths and not missing:
            empty_id = identifier(source["name"], "empty")
            seen.add(empty_id)
            store.failed_source(empty_id, source["name"], "", "no_matching_files", primary)
            failed += 1
        for path in paths:
            source_id = identifier(source["name"], str(path))
            seen.add(source_id)
            try:
                before = path.stat()
                parsed = parse(path, source, config)
                after = path.stat()
                if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                    raise ValueError("source_changed_during_import")
                store.replace_source(source_id, source["name"], str(path), parsed.digest, parsed.events,
                                     {"kind": source["kind"], "raw_lines": parsed.raw_lines, "issues": parsed.issues, "receipt": parsed.receipt,
                                      "matching_roots": provenance.get(str(path), []),
                                      "membership_settings": membership_settings(source),
                                      "role_metadata": "sidecar agentType or explicit configuration", "bytes": before.st_size}, primary)
                imported += 1
            except (OSError, *SHAPE_ERRORS) as exc:
                store.failed_source(source_id, source["name"], str(path), "source_import_failed:" + type(exc).__name__, primary)
                if source_id in known and known[source_id]["status"] == "retired" and known[source_id]["sha256"] is not None:
                    seen.discard(source_id)
                    failed_imports.add(source_id)
                failed += 1
        store.finish_registration(source, seen, primary, roots, failed_imports)
    receipt = {"imported_files": imported, "unavailable_sources": failed, "registered_sources": len(config["sources"]),
               "network_calls": 0, "raw_prompt_storage": False}
    store.set_import_state("complete", receipt)
    return receipt
