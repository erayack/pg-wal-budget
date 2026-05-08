#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_sql="$repo_root/sql/pg_wal_budget--0.1.0.sql"
target_sql="$repo_root/sql/pgrx_bootstrap.sql"

awk '
  NR == 1 && /^\\echo / { next }
  { print }
' "$source_sql" > "$target_sql"
