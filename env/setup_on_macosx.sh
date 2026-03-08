#!/usr/bin/env bash

readonly cwd=$(cd $(dirname $(which $0)); pwd)

## standalone
#osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./standalone/setup.sh"' &

#sleep 5

## sentinel
#osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./sentinel/setup-master.sh"' &
#sleep 5
#osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./sentinel/setup-slave-1.sh"' &
#osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./sentinel/setup-slave-2.sh"' &
#sleep 5
#osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./sentinel/setup-sentinel-1.sh"' &
#osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./sentinel/setup-sentinel-2.sh"' &
#osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./sentinel/setup-sentinel-3.sh"' &

#sleep 5

## cluster
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./cluster/setup-instance-1.sh"' &
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./cluster/setup-instance-2.sh"' &
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./cluster/setup-instance-3.sh"' &
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./cluster/setup-instance-4.sh"' &
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./cluster/setup-instance-5.sh"' &
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./cluster/setup-instance-6.sh"' &
sleep 5
osascript -e 'tell app "Terminal" to do script "cd '${cwd}'; ./cluster/setup-cluster.sh"' &
