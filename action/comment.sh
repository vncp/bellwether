#!/usr/bin/env bash
# Posts the gate's comment on the pull request, or updates the one it posted
# before: the comment by $AUTHOR whose body starts with the marker. Never a
# second comment. Needs GH_TOKEN, REPO (owner/name), PR (the number), and
# AUTHOR; the comment's markdown is the file given.
set -euo pipefail

file=$1
marker='<!-- bellwether-ci -->'
if [ "$(head -n 1 "$file")" != "$marker" ]; then
  echo "::error::$file does not start with $marker"
  exit 2
fi

id=$(gh api --paginate "repos/$REPO/issues/$PR/comments" \
  --jq ".[] | select(.user.login == \"$AUTHOR\" and (.body | startswith(\"$marker\"))) | .id" \
  | head -n 1)

if [ -n "$id" ]; then
  gh api --method PATCH "repos/$REPO/issues/comments/$id" -F "body=@$file" > /dev/null
  echo "updated comment $id"
else
  gh api --method POST "repos/$REPO/issues/$PR/comments" -F "body=@$file" > /dev/null
  echo "posted a comment"
fi
