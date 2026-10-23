# Contributing

- Write code, documentation and commit messages in English.
- Run `python3 -m unittest discover -s tests` before every commit. Keep commits small: at most 15
  files and 500 changed lines, unless the commit message says why.
- Tests and fixtures are synthetic. Never commit real logs, fixtures derived from real logs,
  generated databases or reports, personal paths, private project names or email addresses.
  Keep outputs in `data/`, which Git ignores.
- Before pushing, run `python3 tools/check_public.py --terms <file outside the repo>` and
  `bash tools/check_secrets.sh`. List reviewed Gitleaks exceptions by fingerprint in
  `.gitleaksignore`; a `.gitleaks.toml` is refused.
- Reports show aggregates only, never transcript text or values that look like secrets.
- Agent-hours follow one contract: intervals of one agent are unioned, distinct agents add up, and
  a parent that only waits for its children adds nothing. Three agents working for one hour make
  three agent-hours. A change that moves these numbers needs a test that shows why.
- Separate implemented controls, estimates and verified results in documentation.
- An author's own tests are not a review: changes are reviewed by someone other than their author.
