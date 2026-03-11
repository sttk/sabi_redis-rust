#!/usr/bin/env bash

readonly cwd=$(cd $(dirname $(which $0)); pwd)

## standalone
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./standalone/setup.sh"' &
