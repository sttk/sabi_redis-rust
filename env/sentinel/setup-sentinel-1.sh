#!/usr/bin/env bash

readonly cwd=$(cd $(dirname $(which $0)); pwd)

cp ${cwd}/sentinel-1.conf.orig ${cwd}/sentinel-1.conf

redis-sentinel ${cwd}/sentinel-1.conf
