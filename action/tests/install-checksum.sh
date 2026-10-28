#!/usr/bin/env bash
# install.sh's release-download and checksum verification path against a
# stand-in for `gh release download`, for the host's own release target and,
# on a host that is not Windows, for x86_64-pc-windows-msvc as native Git
# Bash reports it (stand-ins for `uname` and `cygpath`). For each target:
#   (a) a valid checksum passes, unpacks the target's executable (`bellwether`,
#       or `bellwether.exe` on Windows) and nothing else, and appends its bin
#       dir to GITHUB_PATH (in Windows form on Windows);
#   (b) a tampered archive fails with non-zero exit before unpacking;
#   (c) a missing or duplicate entry in checksums.txt fails with exit 2 and
#       the expected error message;
#   (d) an archive whose root does not hold the target's executable name
#       fails with exit 2 and installs nothing.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) host_target=x86_64-unknown-linux-gnu ;;
  Linux-aarch64) host_target=aarch64-unknown-linux-gnu ;;
  Darwin-arm64) host_target=aarch64-apple-darwin ;;
  MINGW*-x86_64|MSYS*-x86_64) host_target=x86_64-pc-windows-msvc ;;
  *) echo "unsupported host $(uname -s)-$(uname -m)" >&2; exit 2 ;;
esac

tag="v0.1.0"
windows_target=x86_64-pc-windows-msvc

mkdir -p "$work/src" "$work/release" "$work/bin" "$work/windows-bin"

# The stand-in binary under both names an archive may hold it by. Git Bash
# runs a file by its contents (a #! line), so a script named bellwether.exe
# runs there as it does on Linux and macOS.
for exe in bellwether bellwether.exe; do
  cat > "$work/src/$exe" <<'FAKE'
#!/usr/bin/env bash
if [ "${1:-}" = "--version" ]; then
  echo "bellwether 0.1.0"
  exit 0
fi
echo "unexpected args: $*" >&2
exit 1
FAKE
  chmod +x "$work/src/$exe"
done

# install.sh checks the archive with `sha256sum -c -`. Where the host's
# sha256sum cannot (none, or macOS's BSD one), a stand-in first on PATH runs
# a checker that can by its absolute path, never `sha256sum` by name, which
# would find the stand-in itself again.
printf 'probe' > "$work/probe"
if ! (cd "$work" && sha256sum probe 2>/dev/null | sha256sum -c - >/dev/null 2>&1); then
  if checker=$(command -v gsha256sum); then
    run_checker=$(printf '%q' "$checker")
  elif checker=$(command -v shasum); then
    run_checker="$(printf '%q' "$checker") -a 256"
  else
    echo "no SHA-256 checker that supports -c on this host" >&2
    exit 2
  fi
  printf '#!/usr/bin/env bash\nexec %s "$@"\n' "$run_checker" > "$work/bin/sha256sum"
  chmod +x "$work/bin/sha256sum"
fi

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

# Native Windows Git Bash as install.sh sees it, for hosts that are not.
cat > "$work/windows-bin/uname" <<'STUB'
#!/usr/bin/env bash
case "$*" in
  -s) echo MINGW64_NT-10.0-20348 ;;
  -m) echo x86_64 ;;
  *) echo "stub uname: unexpected $*" >&2; exit 2 ;;
esac
STUB
cat > "$work/windows-bin/cygpath" <<'STUB'
#!/usr/bin/env bash
[ "$#" -eq 2 ] && [ "$1" = -w ] || { echo "stub cygpath: unexpected $*" >&2; exit 2; }
printf 'windows-form:%s\n' "$2"
STUB
chmod +x "$work/windows-bin/uname" "$work/windows-bin/cygpath"

export PATH="$work/bin:$PATH"
export RELEASE_DIR="$work/release"
export EXPECTED_TAG="$tag"
export EXPECTED_REPO="vncp/bellwether"
export BELLWETHER_VERSION="$tag"
export BELLWETHER_RELEASES="vncp/bellwether"

# pack <member> <asset>: a release archive holding <member> at its root.
pack() {
  (cd "$work/src" && COPYFILE_DISABLE=1 tar -czf - "$1") > "$work/release/$2"
}

# run_install <run>: install.sh with a fresh RUNNER_TEMP and GITHUB_PATH;
# sets `out` and `code`.
run_install() {
  export RUNNER_TEMP="$work/run-$1"
  export GITHUB_PATH="$RUNNER_TEMP/github_path"
  mkdir -p "$RUNNER_TEMP"
  : > "$GITHUB_PATH"
  set +e
  out=$("$here/../install.sh" 2>&1)
  code=$?
  set -e
}

fail() {
  echo "FAIL ($target $1): $2" >&2
  echo "$out" >&2
  exit 1
}

# Nothing unpacked and nothing on GITHUB_PATH.
refused() {
  if [ -n "$(ls -A "$RUNNER_TEMP/bellwether-bin")" ]; then
    fail "$1" "unpacked $(ls -A "$RUNNER_TEMP/bellwether-bin")"
  fi
  if [ -s "$GITHUB_PATH" ]; then
    fail "$1" "GITHUB_PATH was written"
  fi
}

# check <target> <exe>: cases (a) through (d) against the archive of <target>.
check() {
  target=$1
  local exe=$2 asset="bellwether-$tag-$1.tar.gz" bin_entry
  rm -f "$work/release/"*
  pack "$exe" "$asset"
  (cd "$work/release" && sha256sum "$asset" > "$work/good-checksums.txt")

  # (a) Valid checksum passes and puts the executable on GITHUB_PATH.
  cp "$work/good-checksums.txt" "$work/release/checksums.txt"
  run_install "$target-valid"
  [ "$code" -eq 0 ] || fail a "exit $code"
  echo "$out" | grep -qxF "bellwether 0.1.0" || fail a "expected 'bellwether 0.1.0' output"
  [ -x "$RUNNER_TEMP/bellwether-bin/$exe" ] || fail a "unpacked $exe is missing or not executable"
  [ "$(ls -A "$RUNNER_TEMP/bellwether-bin")" = "$exe" ] || fail a "bin dir holds more than $exe"
  if [ "$target" = "$windows_target" ]; then
    bin_entry=$(cygpath -w "$RUNNER_TEMP/bellwether-bin")
  else
    bin_entry="$RUNNER_TEMP/bellwether-bin"
  fi
  [ "$(cat "$GITHUB_PATH")" = "$bin_entry" ] || fail a "GITHUB_PATH is not exactly $bin_entry"

  # (b) Tampered archive fails with non-zero exit before unpacking.
  cp "$work/release/$asset" "$work/good-archive.tar.gz"
  printf 'tampered' >> "$work/release/$asset"
  run_install "$target-tampered"
  cp "$work/good-archive.tar.gz" "$work/release/$asset"
  [ "$code" -ne 0 ] || fail b "tampered archive succeeded"
  refused b

  # (c.1) Missing entry in checksums.txt fails with exit 2 and the expected error message.
  printf '0000000000000000000000000000000000000000000000000000000000000000  bellwether-v0.1.0-other-target.tar.gz\n' \
    > "$work/release/checksums.txt"
  run_install "$target-missing"
  [ "$code" -eq 2 ] || fail c.1 "expected exit 2 for missing checksum entry, got $code"
  echo "$out" | grep -qE "^::error::checksums\\.txt of $tag lists $asset +0 times, not once$" \
    || fail c.1 "unexpected error message"
  refused c.1

  # (c.2) Duplicate entry in checksums.txt fails with exit 2 and the expected error message.
  cat "$work/good-checksums.txt" "$work/good-checksums.txt" > "$work/release/checksums.txt"
  run_install "$target-dup"
  [ "$code" -eq 2 ] || fail c.2 "expected exit 2 for duplicate checksum entry, got $code"
  echo "$out" | grep -qE "^::error::checksums\\.txt of $tag lists $asset +2 times, not once$" \
    || fail c.2 "unexpected error message"
  refused c.2

  # (d) An archive holding the other platform's executable name, correctly
  # checksummed, fails with exit 2 and installs nothing.
  local other=bellwether.exe
  [ "$exe" = bellwether ] || other=bellwether
  pack "$other" "$asset"
  (cd "$work/release" && sha256sum "$asset" > checksums.txt)
  run_install "$target-member"
  [ "$code" -eq 2 ] || fail d "expected exit 2 for an archive without $exe, got $code"
  echo "$out" | grep -qxF "::error::$asset does not hold $exe at its root" || fail d "unexpected error message"
  refused d
}

out=""
if [ "$host_target" = "$windows_target" ]; then
  check "$windows_target" bellwether.exe
  checked="$windows_target (native)"
else
  check "$host_target" bellwether
  host_path=$PATH
  PATH="$work/windows-bin:$host_path"
  check "$windows_target" bellwether.exe
  PATH=$host_path
  checked="$host_target (native), $windows_target (Git Bash stand-ins)"
fi

echo "ok: $checked: release checksum verified, executable unpacked by name, tampered archive, malformed checksums.txt, and wrong member refused"
