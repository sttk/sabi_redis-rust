#!/usr/bin/env bash

readonly cwd=$(cd $(dirname $(which $0)); pwd)

## standalone
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./standalone/setup.sh"' &

## sentinel
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./sentinel/setup-master.sh"' &
sleep 5
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./sentinel/setup-slave-1.sh"' &
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./sentinel/setup-slave-2.sh"' &
sleep 5
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./sentinel/setup-sentinel-1.sh"' &
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./sentinel/setup-sentinel-2.sh"' &
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./sentinel/setup-sentinel-3.sh"' &
