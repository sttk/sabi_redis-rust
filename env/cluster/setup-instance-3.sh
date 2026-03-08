#!/usr/bin/env bash

cwd=$(cd $(dirname $(which $0)); pwd)
cd ${cwd}

redis-server ./7002/redis.conf &
