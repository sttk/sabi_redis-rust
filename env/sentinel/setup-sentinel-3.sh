#!/usr/bin/env bash

readonly cwd=$(cd $(dirname $(which $0)); pwd)

cp ${cwd}/sentinel-3.conf.orig ${cwd}/sentinel-3.conf

redis-sentinel ${cwd}/sentinel-3.conf
