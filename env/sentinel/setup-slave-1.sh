#!/usr/bin/env bash

readonly cwd=$(cd $(dirname $(which $0)); pwd)

redis-server ${cwd}/redis-slave-1.conf
