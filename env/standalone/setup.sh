#!/usr/bin/env bash

readonly cwd=$(cd $(dirname $(which $0)); pwd)

cleanup() {
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
  fi
}

trap cleanup SIGHUP SIGINT SIGTERM

redis-server ${cwd}/redis.conf
pid=$!

wait
