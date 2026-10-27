#!/usr/bin/env bash
# install.sh's release-download and checksum verification path against a
# stand-in for `gh release download`:
#   (a) a valid checksum passes, unpacks `bellwether`, and appends its bin dir
#       to GITHUB_PATH;
#   (b) a tampered archive fails with non-zero exit before unpacking;
#   (c) a missing or duplicate entry in checksums.txt fails with exit 2 and
#       the expected error message.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) host_target=x86_64-unknown-linux-gnu ;;
  Linux-aarch64) host_target=aarch64-unknown-linux-gnu ;;
  Darwin-arm64) host_target=aarch64-apple-darwin ;;
  *) echo "unsupported host $(uname -s)-$(uname -m)" >&2; exit 2 ;;
esac

tag="v0.1.0"
host_asset="bellwether-$tag-$host_target.tar.gz"
linux_asset="bellwether-$tag-x86_64-unknown-linux-gnu.tar.gz"

mkdir -p "$work/src" "$work/release" "$work/bin"
cat > "$work/src/bellwether" <<'FAKE'
#!/usr/bin/env bash
if [ "${1:-}" = "--version" ]; then
  echo "bellwether 0.1.0"
  exit 0
fi
echo "unexpected args: $*" >&2
exit 1
FAKE
chmod +x "$work/src/bellwether"

# Pack both bellwether-v0.1.0-x86_64-unknown-linux-gnu.tar.gz and the current
# host's target archive with `bellwether` at the archive root.
COPYFILE_DISABLE=1 tar -C "$work/src" -czf "$work/release/$linux_asset" bellwether
if [ "$host_asset" != "$linux_asset" ]; then
  cp "$work/release/$linux_asset" "$work/release/$host_asset"
fi

# Ensure a GNU-compatible `sha256sum` (supporting `-c -`) is first on PATH on
# macOS hosts where `/sbin/sha256sum` is BSD digest without `-c`.
cat > "$work/bin/sha256sum" <<'SHASUM'
#!/usr/bin/env bash
if command -v gsha256sum >/dev/null 2>&1; then
  exec gsha256sum "$@"
elif command -v shasum >/dev/null 2>&1; then
  exec shasum -a 256 "$@"
else
  exec /usr/bin/env sha256sum "$@"
fi
SHASUM
chmod +x "$work/bin/sha256sum"

cat > "$work/bin/gh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
[ "${1:-}" = release ] && [ "${2:-}" = download ] || { echo "stub gh: expected release download, got $*" >&2; exit 2; }
req_tag=$3
shift 3
repo=""
dir=""
clobber=0
patterns=()
while [ $# -gt 0 ]; do
  case "$1" in
    --repo) repo=$2; shift 2 ;;
    --pattern) patterns+=("$2"); shift 2 ;;
    --dir) dir=$2; shift 2 ;;
    --clobber) clobber=1; shift ;;
    *) echo "stub gh: unexpected arg $1" >&2; exit 2 ;;
  esac
done
[ "$req_tag" = "$EXPECTED_TAG" ] || { echo "stub gh: unexpected tag $req_tag" >&2; exit 2; }
[ "$repo" = "$EXPECTED_REPO" ] || { echo "stub gh: unexpected repo $repo" >&2; exit 2; }
[ "$clobber" -eq 1 ] || { echo "stub gh: expected --clobber" >&2; exit 2; }
for p in "${patterns[@]}"; do
  cp "$RELEASE_DIR/$p" "$dir/$p"
done
STUB
chmod +x "$work/bin/gh"

export PATH="$work/bin:$PATH"
export RELEASE_DIR="$work/release"
export EXPECTED_TAG="$tag"
export EXPECTED_REPO="vncp/bellwether"
export BELLWETHER_VERSION="$tag"
export BELLWETHER_RELEASES="vncp/bellwether"

(cd "$work/release" && "$work/bin/sha256sum" "$host_asset" > "$work/good-checksums.txt")

# (a) Valid checksum passes and puts `bellwether` on GITHUB_PATH.
cp "$work/good-checksums.txt" "$work/release/checksums.txt"
export RUNNER_TEMP="$work/run-valid"
export GITHUB_PATH="$work/run-valid/github_path"
mkdir -p "$RUNNER_TEMP"
: > "$GITHUB_PATH"

out=$("$here/../install.sh" 2>&1)
echo "$out" | grep -qxF "bellwether 0.1.0" || {
  echo "FAIL (a): expected 'bellwether 0.1.0' output, got: $out" >&2
  exit 1
}
grep -qxF "$RUNNER_TEMP/bellwether-bin" "$GITHUB_PATH" || {
  echo "FAIL (a): $RUNNER_TEMP/bellwether-bin not written to GITHUB_PATH" >&2
  exit 1
}
[ -x "$RUNNER_TEMP/bellwether-bin/bellwether" ] || {
  echo "FAIL (a): unpacked bellwether is missing or not executable" >&2
  exit 1
}

# (b) Tampered archive fails with non-zero exit before unpacking.
cp "$work/release/$host_asset" "$work/good-archive.tar.gz"
printf 'tampered' >> "$work/release/$host_asset"
export RUNNER_TEMP="$work/run-tampered"
export GITHUB_PATH="$work/run-tampered/github_path"
mkdir -p "$RUNNER_TEMP"
: > "$GITHUB_PATH"

set +e
out=$("$here/../install.sh" 2>&1)
code=$?
set -e
cp "$work/good-archive.tar.gz" "$work/release/$host_asset"
if [ "$code" -eq 0 ]; then
  echo "FAIL (b): tampered archive succeeded" >&2
  exit 1
fi
if [ -e "$RUNNER_TEMP/bellwether-bin/bellwether" ]; then
  echo "FAIL (b): tampered archive was unpacked despite checksum mismatch" >&2
  exit 1
fi
if [ -s "$GITHUB_PATH" ]; then
  echo "FAIL (b): GITHUB_PATH was modified on tampered archive" >&2
  exit 1
fi

# (c.1) Missing entry in checksums.txt fails with exit 2 and the expected error message.
printf '0000000000000000000000000000000000000000000000000000000000000000  bellwether-v0.1.0-other-target.tar.gz\n' \
  > "$work/release/checksums.txt"
export RUNNER_TEMP="$work/run-missing"
export GITHUB_PATH="$work/run-missing/github_path"
mkdir -p "$RUNNER_TEMP"
: > "$GITHUB_PATH"

set +e
out=$("$here/../install.sh" 2>&1)
code=$?
set -e
if [ "$code" -ne 2 ]; then
  echo "FAIL (c.1): expected exit 2 for missing checksum entry, got $code ($out)" >&2
  exit 1
fi
echo "$out" | grep -qE "^::error::checksums\\.txt of $tag lists $host_asset +0 times, not once$" || {
  echo "FAIL (c.1): unexpected error message: $out" >&2
  exit 1
}
if [ -e "$RUNNER_TEMP/bellwether-bin/bellwether" ]; then
  echo "FAIL (c.1): archive was unpacked when checksum entry was missing" >&2
  exit 1
fi

# (c.2) Duplicate entry in checksums.txt fails with exit 2 and the expected error message.
cat "$work/good-checksums.txt" "$work/good-checksums.txt" > "$work/release/checksums.txt"
export RUNNER_TEMP="$work/run-dup"
export GITHUB_PATH="$work/run-dup/github_path"
mkdir -p "$RUNNER_TEMP"
: > "$GITHUB_PATH"

set +e
out=$("$here/../install.sh" 2>&1)
code=$?
set -e
if [ "$code" -ne 2 ]; then
  echo "FAIL (c.2): expected exit 2 for duplicate checksum entry, got $code ($out)" >&2
  exit 1
fi
echo "$out" | grep -qE "^::error::checksums\\.txt of $tag lists $host_asset +2 times, not once$" || {
  echo "FAIL (c.2): unexpected error message: $out" >&2
  exit 1
}
if [ -e "$RUNNER_TEMP/bellwether-bin/bellwether" ]; then
  echo "FAIL (c.2): archive was unpacked when checksum entry was duplicated" >&2
  exit 1
fi

echo "ok: release checksum verified, tampered archive and malformed checksums.txt refused"
