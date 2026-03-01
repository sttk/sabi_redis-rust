#!/usr/bin/env bash

readonly cwd=$(cd $(dirname $(which $0)); pwd)

redis-server ${cwd}/redis-master.conf
