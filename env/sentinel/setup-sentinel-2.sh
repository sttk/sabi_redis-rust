#!/usr/bin/env bash

readonly cwd=$(cd $(dirname $(which $0)); pwd)

cp ${cwd}/sentinel-2.conf.orig ${cwd}/sentinel-2.conf

redis-sentinel ${cwd}/sentinel-2.conf
