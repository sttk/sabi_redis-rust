#!/usr/bin/env bash

readonly cwd=$(cd $(dirname $(which $0)); pwd)

redis-sentinel ${cwd}/sentinel-3.conf
