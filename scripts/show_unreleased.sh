#!/bin/bash
set -euo pipefail

RED="\033[0;31m"
GREEN="\033[0;32m"
RESET="\033[0m"

last_tag=$(git tag | grep -v 'v' | sort -Vr | head -n 1)
echo "Last tag: $last_tag"
echo

git log "$last_tag"..HEAD --pretty=format:'%H %s' | while read -r commit_hash commit_msg; do
    note=$(git notes --ref=next show "$commit_hash" 2>/dev/null || true)
    if [[ -z "$note" ]]; then
        echo -e "${RED}* $commit_hash - $commit_msg (no note)${RESET}"
    else
        echo -e "${GREEN}* $commit_hash - $commit_msg${RESET}"
    fi
done

