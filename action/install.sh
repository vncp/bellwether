#!/usr/bin/env bash
# Puts `bellwether` on the job's PATH: a release's binary, checked against
# the SHA-256 in that release's checksums.txt before it is unpacked, or
# (BELLWETHER_VERSION=build) the bellwether at this action's own ref,
# compiled with cargo. Never code from the pull request.
#
# Runs under Linux, macOS, and native Windows Git Bash (the `bash` shell of
# a windows-latest runner), where the executable is bellwether.exe. WSL is
# Linux and gets the Linux binary.
set -euo pipefail

os=$(uname -s)
arch=$(uname -m)
case "$os" in
  MINGW*|MSYS*) windows=1; exe=bellwether.exe ;;
  *) windows=0; exe=bellwether ;;
esac

bin="$RUNNER_TEMP/bellwether-bin"
mkdir -p "$bin"

if [ "$BELLWETHER_VERSION" = build ]; then
  case "$BELLWETHER_BUILD_PROFILE" in
    release) profile=() ;;
    debug) profile=(--debug) ;;
    *) echo "::error::build-profile must be release or debug, not $BELLWETHER_BUILD_PROFILE"; exit 2 ;;
  esac
  cargo install --locked ${profile[@]+"${profile[@]}"} --path "$GITHUB_ACTION_PATH/../crates/bellwether-cli" \
    --root "$RUNNER_TEMP/bellwether-install"
  cp "$RUNNER_TEMP/bellwether-install/bin/$exe" "$bin/"
else
  if ! [[ "$BELLWETHER_VERSION" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "::error::version must be a release tag or build, not $BELLWETHER_VERSION"
    exit 2
  fi
  case "$os-$arch" in
    Linux-x86_64) target=x86_64-unknown-linux-gnu ;;
    Linux-aarch64) target=aarch64-unknown-linux-gnu ;;
    Darwin-arm64) target=aarch64-apple-darwin ;;
    MINGW*-x86_64|MSYS*-x86_64) target=x86_64-pc-windows-msvc ;;
    *) echo "::error::no bellwether release binary for $os $arch; use version: build"; exit 2 ;;
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
  # The archive holds the executable at its root. It goes in on stdin: Git
  # Bash's GNU tar reads an archive named D:\... as a file on host D.
  if ! (cd "$bin" && tar -xzf - "$exe") < "$dl/$asset"; then
    echo "::error::$asset does not hold $exe at its root"
    exit 2
  fi
fi

chmod +x "$bin/$exe"
"$bin/$exe" --version
# The runner reads GITHUB_PATH as a Windows path on Windows.
if [ "$windows" = 1 ]; then
  cygpath -w "$bin" >> "$GITHUB_PATH"
else
  echo "$bin" >> "$GITHUB_PATH"
fi
