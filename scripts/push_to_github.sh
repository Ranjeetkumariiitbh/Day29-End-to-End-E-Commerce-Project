#!/usr/bin/env bash
# Push this project to GitHub as Day29-End-to-End-E-Commerce-Project.
# Usage: ./scripts/push_to_github.sh <github-username>
set -euo pipefail

REPO="Day29-End-to-End-E-Commerce-Project"
USER_NAME="${1:?usage: $0 <github-username>}"

if [ ! -d .git ]; then git init -b main && git add -A && git commit -m "Day 29: end-to-end e-commerce Spark pipeline"; fi

if command -v gh >/dev/null 2>&1; then
  gh repo create "$REPO" --public --source=. --remote=origin --push \
    --description "Day 29: End-to-end e-commerce data pipeline with Apache Spark (Scala) - Raw/Clean/Enrich/Aggregate"
else
  git remote remove origin 2>/dev/null || true
  git remote add origin "https://github.com/${USER_NAME}/${REPO}.git"
  git branch -M main
  git push -u origin main   # create the empty repo on github.com first
fi
