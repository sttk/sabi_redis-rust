#!/usr/bin/env bash

cwd=$(cd $(dirname $(which $0)); pwd)
cd ${cwd}

ulimit -n 8192

redis-server ./7004/redis.conf
