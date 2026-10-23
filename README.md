# workflow-auditor

An offline accounting tool for agent workflows. It reads the local logs that Claude Code, Codex and
GLM attempt reports leave behind, stores accounting metadata in SQLite, and reports usage, dated
list-price equivalents and **agent-hours** by date, repository, provider, model, role and agent.
It makes no network calls and never stores prompts, responses or tool arguments.

It is not a continuous collector, a census of every repository on a machine, or a reconciliation
with invoices or subscription quota. Figures in list-price units are equivalents under a dated
price book, not money charged.

## Requirements and checks

Python 3.11 or later, standard library only (SQLite included). Developed on Python 3.12 under
Linux and WSL2. From the repository root:

```bash
python3 -m unittest discover -s tests
python3 demo_parallelism.py
```

`demo_parallelism.py` reproduces the agent-hour example below with synthetic events.

## Quick start with the synthetic example

`examples/history/` holds a small synthetic history: a Claude Code session with one reviewer
subagent, a Codex rollout and a GLM attempt report, all under the repository name `example-app`.

```bash
mkdir -p data
python3 audit.py ingest --config examples/history/sources.json --db data/example.sqlite
python3 audit.py report --db data/example.sqlite --group-by provider,role --format markdown
python3 validate_history.py --fixtures examples/history --db data/example.sqlite
```

Run these from the repository root, since the example's roots are relative to it.
`validate_history.py` compares a database with frozen references: token sums per case in
`history-fixtures.json`, and list-price totals in `cost-fixtures.json`. It opens the database
read-only and exits 1 if any check fails. Use the same script with your own fixtures to guard
an accounting history against regressions.

## Importing your own logs

```bash
python3 audit.py init-config --output my-sources.json
# Edit roots, repository aliases, roles and the price book in my-sources.json.
python3 audit.py ingest --config my-sources.json --db data/my-workflow.sqlite
python3 audit.py report --db data/my-workflow.sqlite --group-by repo,provider,role --format markdown
```

The starting configuration registers `~/.claude/projects` and `~/.codex/sessions`. Add Windows
roots explicitly if clients run there; both `C:/Users/...` and `/mnt/c/Users/...` are accepted.
`ingest` checks the configuration before it creates or opens a database. It reads the sources and
writes only the chosen database. A root that is missing or has no matching files is reported as
partial coverage. Re-importing replaces the observations of the same file and deduplicates copies
by accounting identity. `report` and `compare` open the database read-only. Keep generated
databases and reports out of version control; `data/` is ignored for that purpose.

A further source registration inside `sources`:

```json
{
  "name": "my-glm-attempts",
  "kind": "glm_report",
  "roots": ["/path/to/project/agent-runs"],
  "pattern": "**/report.json",
  "repo": "my-project",
  "provider": "z.ai"
}
```

Do not register an aggregate view and the detailed logs of the same run as primary sources. The
auditor holds back overlaps it can identify by provider, session and model; it cannot find
duplicates across systems that share no identity. The `router` adapter corroborates by default and
adds nothing to totals.

## Dates, roles, agents and the agent tree

Dates need an explicit zone: `2026-10-20T07:00:00-03:00` is `2026-10-20T10:00:00Z`. Ranges are
half-open, `[start, end)`. A detailed token record is placed at its own timestamp; tokens are never
spread over minutes.

```bash
# Find the identifiers recorded in a period.
python3 audit.py report --db data/my-workflow.sqlite --repo my-project \
  --since 2026-10-20T00:00:00Z --until 2026-10-21T00:00:00Z --group-by agent,role

# One role; repeated filters of the same kind combine with OR.
python3 audit.py report --db data/my-workflow.sqlite --repo my-project --role reviewer --group-by agent

# One agent and its known descendants.
python3 audit.py report --db data/my-workflow.sqlite --agent 'claude:SESSION:AGENT' \
  --descendants --group-by agent,role

# Two periods under the same activity method.
python3 audit.py compare --db data/my-workflow.sqlite --repo my-project --group-by role \
  --before-start 2026-10-16T00:00:00Z --before-end 2026-10-18T00:00:00Z \
  --after-start 2026-10-18T00:00:00Z --after-end 2026-10-20T00:00:00Z --format markdown
```

Filters of different kinds combine with AND. `--agent` takes a full ID or a `:`-separated suffix;
use the full ID to avoid matching several instances. `--descendants` walks the **known**
descendants without repeating nodes; cycles and contradictory parents are reported, and missing
relations are not reconstructed.

`repo_aliases` maps folder prefixes, clones and worktrees included, to stable names; the most
specific prefix wins. Without an alias or an explicit `repo`, the Git root of the recorded working
directory is used, and otherwise the repository is `unattributed`.

Roles come from configuration and native metadata only, never from reading prompts. `agent_roles`
assigns a role by full agent ID. Claude subagent types `reviewer`, `implementer` and `scout` map to
those roles; `type_roles` maps any other `agentType`. The main session is labelled `lead` by
structure. `general-purpose` stays `unknown` unless configuration assigns it.

## Agent-hours

Three agents that each work for one hour make **3 agent-hours**, though one hour of project time
passes. Intervals of one agent are unioned; distinct agents add up, the parent's own work included;
a parent that only waits for its children adds nothing. Cost per agent-hour measures intensity, not
efficiency per result: a slower agent can look cheaper per hour.

There are two methods, and a comparison must use the same one in both periods:

- `--activity buckets`, the default: each timestamp occupies a 600-second UTC bucket. Buckets of
  one agent are unioned and those of different agents add up, clipped at the window's edges. **It
  is an approximation**, not measured run time, CPU time or human work. `--bucket-seconds 300` or
  `900` shows the sensitivity.
- `--activity observed`: only intervals marked `activity_basis: "own"` in `normalized` input. They
  must measure the agent's own activity; parent durations that include waiting for children are
  not imported, and a session's wall-clock span is not read as activity.

A GLM `report.json` or a stream's final `modelUsage` gives token totals but no agent-hours. The
total and the rates that depend on it are then `null`, and the known subtotal stays available.
Wall-clock time never fills the gap. An aggregate that crosses a window edge is held back as
missing, never prorated.

Groups by model, provider, role or repository can share one agent's bucket, so the global summary
is computed separately; `group_activity_totals_are_additive` says whether the rows may be added.

## Adapters

| `kind` | Source | Unit and limits |
| --- | --- | --- |
| `claude` | Native JSONL under `.claude/projects`, with the `.meta.json` sidecar | One response. Progressive blocks and copies are merged. Roles from `agentType` or explicit maps. |
| `codex` | Native rollout JSONL | Delta of cumulative counters; keeps the first identity and native parent links. Unresolved counter resets are reported. |
| `glm_report` | `report.json` of registered attempts | Total per session plus attempt start. Distinct resumes are not merged. No per-agent activity. |
| `claude_stream` | `--output-format stream-json` with `result.modelUsage` | Final total per model, helper models included once. No time detail. |
| `router` | JSONL of requests with ID and tokens | Corroborating by default, left out of totals; primary only for an exclusive source of known coverage. |
| `normalized` | JSONL events in the `workflow_auditor.model.Event` schema | Input for explicit measurement and the synthetic demo. |

What counts as one event depends on the adapter and is listed in `event_units`; it is not a plan
prompt, a tool call or a human turn. Client and provider are kept apart: Codex running GLM is client
`codex` and provider `z.ai`. The attribution basis (registration, native metadata, or inference from
the model name) is published; it does not prove which account was billed.

### Copies of one Claude response

A resumed Claude Code session rewrites earlier responses into its own transcript under its own
session ID. Copies of one response with the same accounting counters are counted once and given to
one owner, chosen in this order:

1. The earliest timestamp wins.
2. On a tie, a tied run that holds everything another tied run holds, and adds only responses at or
   after that run's latest one, is a copy and drops out.
3. Among the runs still contending, the one whose own activity began first wins: its earliest
   response that no other contending run holds.
4. Last, the smallest attribution tuple wins.

Reassigned copies are reported as `copied_responses_reattributed` and do not lower trust. Copies
that differ only in a known `speed` or `inference_geo` are kept unpriced and listed under
`coverage.speed_geo_mismatches`. Any other difference between copies (counters, client, provider,
model, or attribution outside these rules) is a conflict and makes the report `partial`. A Claude
row without `message.id` or `requestId` is a `missing_response_identity` issue.

## Prices, coverage and statuses

`prices-20261022.json` is a dated list-price book, never updated from the Internet.
`--prices other-book.json` revalues the same selection under another book. Each model declares
rates per million tokens and a unit; USD and credits are never added together. Models without a
rate stay unpriced and keep their tokens; GLM has no invented dollar rate. For Claude Code, fast
mode doubles the rates and `inference_geo: "us"` multiplies them by 1.1. A response with known zero
usage is priced at 0. CLI-reported cost, list equivalent, billed money and quota deduction are
separate fields; the last two stay `null`.

Input totals include fresh, cache-read and cache-write tokens; output totals include reasoning
when the source reports it. An unknown component is never replaced by zero. Context-dependent
rates, or cache writes without a known TTL, produce a range.

Reports publish coverage, source hashes, missing counters, attribution and conflicts; they never
include transcript text. Statuses:

- `ready`: the selection has every supported metric within the registered sources.
- `ready_with_missing_metrics`: some metric is missing, such as activity or a price.
- `partial`: a source, reconciliation, time or import problem. Known subtotals are shown, and
  totals and rates that would imply full coverage are withheld, agent-hours, project hours and
  parallelism included. A problem in a primary source can withhold rates for other selections of
  the same database; that is a deliberate, conservative choice.

Queries read one SQLite snapshot, each file's update is transactional, and an interrupted import
keeps the report `partial`. One importer runs at a time; this is not a concurrent multi-host
collector. A database written by an earlier version keeps its import-time decisions until it is
re-imported.

Out of scope for now: continuous capture, balance and invoice reconciliation, automatic joins
between clients without shared IDs, accepted outcomes, quota accounting and a web dashboard.

## Publishing safely

Before pushing, run both checks from the repository root:

```bash
python3 tools/check_public.py --terms /path/outside/the/repo/private-terms.txt
bash tools/check_secrets.sh
```

`check_public.py` fails on personal paths, email addresses other than noreply ones, and any term
from a private list kept outside the repository, across all history, staged files and commit
metadata. `check_secrets.sh` runs a pinned, checksum-verified Gitleaks over every commit.
