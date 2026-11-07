#!/usr/bin/env bash
# The server's HTTP integration tests, against a database that is thrown away
# only when there is not one to reuse. Anything after the script's name goes to
# cargo, so `./scripts/test-api.sh relay` runs the tests whose names match.
set -euo pipefail
cd "$(dirname "$0")/.."

# A Postgres installation, not a running one: this may have to make a cluster.
# `PG_BIN` names the directory holding initdb and pg_ctl when they are not on
# PATH — on Windows the installer leaves them out of it.
if [ -z "${PG_BIN:-}" ]; then
  candidates=()
  if command -v pg_config >/dev/null 2>&1; then
    candidates+=("$(pg_config --bindir)")
  fi
  candidates+=(/usr/lib/postgresql/*/bin /usr/local/pgsql/bin /opt/homebrew/opt/postgresql*/bin)
  # The Windows installer leaves its binaries out of PATH, and its path has a space.
  candidates+=("/c/Program Files/PostgreSQL"/*/bin)
  for dir in "${candidates[@]}"; do
    if [ -x "$dir/initdb" ] || [ -x "$dir/initdb.exe" ]; then
      PG_BIN=$dir
      break
    fi
  done
fi
if [ -z "${PG_BIN:-}" ]; then
  echo "no initdb found: install Postgres, or set PG_BIN to the directory holding it" >&2
  exit 1
fi

# Called with a database already chosen, there is nothing to make or decide.
if [ -n "${TEST_DATABASE_URL:-}" ]; then
  echo "using TEST_DATABASE_URL as it is"
  exec cargo test -p tradoor-server --test api "$@"
fi

# The cluster `tradoor-server` itself uses, if this machine has one: starting it
# takes a moment where initdb takes several, and each run keeps the migrations the
# last one applied. `TRADOOR_PGDATA` and `TRADOOR_PGPORT` say where it is.
data=${TRADOOR_PGDATA:-}
if [ -z "$data" ]; then
  for candidate in "${LOCALAPPDATA:-}/tradoor/pgdata" "${XDG_DATA_HOME:-$HOME/.local/share}/tradoor/pgdata"; do
    if [ -d "$candidate" ]; then
      data=$candidate
      break
    fi
  done
fi
port=${TRADOOR_PGPORT:-5438}

if [ -n "$data" ] && [ -d "$data" ]; then
  if ! "$PG_BIN/pg_isready" -h 127.0.0.1 -p "$port" >/dev/null 2>&1; then
    "$PG_BIN/pg_ctl" -D "$data" -l "$(dirname "$data")/pg.log" -o "-p $port -h 127.0.0.1" -w start >/dev/null 2>&1 || {
      echo "the cluster at $data would not start on port $port:" >&2
      tail -5 "$(dirname "$data")/pg.log" >&2 || true
      exit 1
    }
    echo "started the cluster at $data on 127.0.0.1:$port"
  fi
  # A database of its own: the tests write rows, and none of them belong beside
  # the ones the charts are keeping.
  test_db=tradoor_test
  if [ "$("$PG_BIN/psql" -w -h 127.0.0.1 -p "$port" -U postgres -tAc "select 1 from pg_database where datname = '$test_db'")" != "1" ]; then
    "$PG_BIN/createdb" -h 127.0.0.1 -p "$port" -U postgres "$test_db"
  fi
  echo "Postgres on 127.0.0.1:$port, database $test_db, kept between runs"
  TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$port/$test_db" cargo test -p tradoor-server --test api "$@"
  exit
fi

# No cluster to borrow: one for this run, removed when it ends.
work=$(mktemp -d)
data=$work/pg
cleanup() {
  status=$?
  "$PG_BIN/pg_ctl" -D "$data" -m immediate -w stop >"$work/stopt.log" 2>&1 || true
  rm -rf "$work"
  exit $status
}
trap cleanup EXIT INT TERM

# A cluster of its own, trusting loopback and nothing else, so no password is needed.
"$PG_BIN/initdb" -D "$data" -U postgres --auth=trust --encoding=UTF8 --locale=C >"$work/initdb.log" 2>&1

# Any free port will do; the range is one nothing else usually wants, and a port
# already in use fails the start rather than being retried forever.
port=$((55432 + RANDOM % 100))
for attempt in 1 2 3 4 5; do
  if "$PG_BIN/pg_ctl" -D "$data" -l "$work/pg.log" -o "-p $port -h 127.0.0.1" -w start >"$work/start.log" 2>&1; then
    break
  fi
  if [ "$attempt" = 5 ]; then
    echo "Postgres would not start on ports $((port - 4)) to $port:" >&2
    cat "$work/pg.log" >&2
    exit 1
  fi
  port=$((port + 1))
done
"$PG_BIN/createdb" -h 127.0.0.1 -p "$port" -U postgres tradoor_it

echo "Postgres $("$PG_BIN/postgres" --version | cut -d' ' -f3) on 127.0.0.1:$port, thrown away afterwards"
TEST_DATABASE_URL="postgres://postgres@127.0.0.1:$port/tradoor_it" cargo test -p tradoor-server --test api "$@"
