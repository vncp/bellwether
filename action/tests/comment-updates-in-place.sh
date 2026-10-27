#!/usr/bin/env bash
# comment.sh against a stand-in for `gh api` that keeps a pull request's
# comments in a JSON file: two pushes leave one bellwether comment, holding
# the second verdict, and another author's comment carrying the same
# marker is never touched. Needs jq.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

mkdir -p "$work/bin"
cat > "$work/bin/gh" <<'STUB'
#!/usr/bin/env bash
# gh api, as comment.sh calls it, over $STORE (a JSON array of comments).
set -euo pipefail
[ "$1" = api ] || { echo "stub gh: only api" >&2; exit 2; }
shift
method=GET jq_filter='.' body='' path=''
while [ $# -gt 0 ]; do
  case "$1" in
    --paginate) shift ;;
    --method) method=$2; shift 2 ;;
    --jq) jq_filter=$2; shift 2 ;;
    -F) body=$(cat "${2#body=@}"); shift 2 ;;
    *) path=$1; shift ;;
  esac
done
case "$method $path" in
  "GET repos/o/r/issues/7/comments")
    jq -r "$jq_filter" "$STORE" ;;
  "POST repos/o/r/issues/7/comments")
    jq --arg b "$body" --arg a "$AUTHOR" \
      '. + [{id: ((map(.id) | max // 0) + 1), user: {login: $a}, body: $b}]' "$STORE" > "$STORE.new"
    mv "$STORE.new" "$STORE" ;;
  PATCH\ repos/o/r/issues/comments/*)
    id=${path##*/}
    jq --arg b "$body" --argjson id "$id" 'map(if .id == $id then .body = $b else . end)' \
      "$STORE" > "$STORE.new"
    mv "$STORE.new" "$STORE" ;;
  *) echo "stub gh: unexpected $method $path" >&2; exit 2 ;;
esac
STUB
chmod +x "$work/bin/gh"

export PATH="$work/bin:$PATH" STORE="$work/comments.json" REPO=o/r PR=7 AUTHOR='github-actions[bot]'
# Someone quoting the marker in their own comment.
jq -n '[{id: 1, user: {login: "someone"}, body: "<!-- bellwether-ci -->\nquoted"}]' > "$STORE"

printf '<!-- bellwether-ci -->\n### ❌ 1 of 1 step failed the receipt gate\n' > "$work/first.md"
printf '<!-- bellwether-ci -->\n### ✅ 1 step verified\n' > "$work/second.md"
"$here/../comment.sh" "$work/first.md"
"$here/../comment.sh" "$work/second.md"

mine=$(jq '[.[] | select(.user.login == "github-actions[bot]")] | length' "$STORE")
if [ "$mine" -ne 1 ]; then
  echo "FAIL: $mine bellwether comments after two pushes, not 1" >&2
  jq . "$STORE" >&2
  exit 1
fi
jq -e '.[] | select(.user.login == "github-actions[bot]") | .body | contains("1 step verified")' "$STORE" > /dev/null \
  || { echo "FAIL: the comment does not hold the second verdict" >&2; exit 1; }
jq -e '.[] | select(.id == 1) | .body == "<!-- bellwether-ci -->\nquoted"' "$STORE" > /dev/null \
  || { echo "FAIL: another author's comment was changed" >&2; exit 1; }
echo "ok: one comment, updated in place"
