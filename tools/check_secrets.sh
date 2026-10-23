#!/usr/bin/env bash
# Scan every commit of every local branch for secrets with a pinned, checksum-verified Gitleaks.
# Nothing is installed globally; exceptions go in .gitleaksignore as reviewed fingerprints.
set -euo pipefail

if [[ $(uname -s) != Linux || $(uname -m) != x86_64 ]]; then
  echo 'Secret scanning requires Linux x86_64 (WSL2 or Ubuntu).' >&2
  exit 1
fi

secrets_script=$(realpath -- "${BASH_SOURCE[0]}")
cd -- "$(dirname -- "$secrets_script")/.."
# Exceptions are reviewed fingerprints in .gitleaksignore; a config or inline comment could hide a leak.
if [[ -e .gitleaks.toml ]]; then
  echo 'Remove .gitleaks.toml: list reviewed exceptions by fingerprint in .gitleaksignore.' >&2
  exit 1
fi

secrets_tmp=$(mktemp -d)
trap 'rm -rf -- "$secrets_tmp"' EXIT

download() {
  local url=$1 destination=$2 digest=$3
  curl -q --fail --silent --show-error --location --proto '=https' \
    --connect-timeout 10 --max-time 60 --max-filesize 16777216 \
    "$url" --output "$destination"
  if ! printf '%s  %s\n' "$digest" "$destination" | sha256sum --check --status; then
    echo "Checksum mismatch: ${destination##*/}" >&2
    return 1
  fi
}

# Reviewed official release asset; update the version and digest together.
gitleaks_version=8.30.1
download "https://github.com/gitleaks/gitleaks/releases/download/v${gitleaks_version}/gitleaks_${gitleaks_version}_linux_x64.tar.gz" \
  "$secrets_tmp/gitleaks.tar.gz" 551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb
# Read only the named executable; archive paths never become filesystem destinations.
tar -xzOf "$secrets_tmp/gitleaks.tar.gz" gitleaks > "$secrets_tmp/gitleaks"
chmod 700 "$secrets_tmp/gitleaks"
# Committed content is what a push publishes; ignored local files are not scanned.
timeout 300 env -u GITLEAKS_CONFIG -u GITLEAKS_CONFIG_TOML "$secrets_tmp/gitleaks" git \
  --no-banner --verbose --redact=100 --log-level warn --ignore-gitleaks-allow \
  --log-opts='--all --full-history' .
echo 'Secrets in committed content and history: none found'
