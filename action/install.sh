#!/usr/bin/env bash
# Puts `bellwether` on the job's PATH: a release's binary, checked against
# the SHA-256 in that release's checksums.txt before it is unpacked, or
# (BELLWETHER_VERSION=build) the bellwether at this action's own ref,
# compiled with cargo. Never code from the pull request.
set -euo pipefail

bin="$RUNNER_TEMP/bellwether-bin"
mkdir -p "$bin"

if [ "$BELLWETHER_VERSION" = build ]; then
  case "$BELLWETHER_BUILD_PROFILE" in
    release) profile=() ;;
    debug) profile=(--debug) ;;
    *) echo "::error::build-profile must be release or debug, not $BELLWETHER_BUILD_PROFILE"; exit 2 ;;
  esac
  cargo install --locked "${profile[@]}" --path "$GITHUB_ACTION_PATH/../crates/bellwether-cli" \
    --root "$RUNNER_TEMP/bellwether-install"
  cp "$RUNNER_TEMP/bellwether-install/bin/bellwether" "$bin/"
else
  if ! [[ "$BELLWETHER_VERSION" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "::error::version must be a release tag or build, not $BELLWETHER_VERSION"
    exit 2
  fi
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) target=x86_64-unknown-linux-gnu ;;
    Linux-aarch64) target=aarch64-unknown-linux-gnu ;;
    Darwin-arm64) target=aarch64-apple-darwin ;;
    *) echo "::error::no bellwether release binary for $(uname -s) $(uname -m); use version: build"; exit 2 ;;
  esac
  asset="bellwether-$BELLWETHER_VERSION-$target.tar.gz"
  dl="$RUNNER_TEMP/bellwether-download"
  mkdir -p "$dl"
  gh release download "$BELLWETHER_VERSION" --repo "$BELLWETHER_RELEASES" \
    --pattern "$asset" --pattern checksums.txt --dir "$dl" --clobber
  # Exactly one line names the asset, and its hash must match.
  lines=$(awk -v a="$asset" '$2 == a || $2 == "*" a' "$dl/checksums.txt" | wc -l)
  if [ "$lines" -ne 1 ]; then
    echo "::error::checksums.txt of $BELLWETHER_VERSION lists $asset $lines times, not once"
    exit 2
  fi
  (cd "$dl" && awk -v a="$asset" '$2 == a || $2 == "*" a { print $1 "  " a }' checksums.txt | sha256sum -c -)
  tar -xzf "$dl/$asset" -C "$bin" bellwether
fi

chmod +x "$bin/bellwether"
"$bin/bellwether" --version
echo "$bin" >> "$GITHUB_PATH"
