#!/usr/bin/env bash
set -euo pipefail

# Pre-commit passes staged filenames as args. We only block root-level test*.py.
for file in "$@"; do
  name="$(basename "$file")"
  dir="$(dirname "$file")"
  if [[ "$dir" == "." && "$name" == test*.py ]]; then
    echo "Root test file is not allowed: $file"
    echo "Place tests under tests/ with *_test.py or test_*.py"
    exit 1
  fi
done

exit 0

