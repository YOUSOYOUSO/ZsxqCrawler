#!/usr/bin/env bash
set -euo pipefail

# Block new root-level Python files except explicit allowlist.
for file in "$@"; do
  dir="$(dirname "$file")"
  name="$(basename "$file")"
  if [[ "$dir" != "." || "$name" != *.py ]]; then
    continue
  fi

  # no root-level python files are allowed in this repository layout.
  echo "Root-level Python file is not allowed: $file"
  echo "Move it under app/, modules/, scripts/, or tests/."
  exit 1
done

exit 0
