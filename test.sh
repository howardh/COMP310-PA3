rm -rf d1/* d2/*
build/namenode 50070 > /dev/null &
pidnn=$!
sleep 3 
build/datanode 50060 127.0.0.1 1 d1/ > /dev/null & 
piddn1=$!
build/datanode 50061 127.0.0.1 2 d2/ > /dev/null & 
piddn2=$!
./dfs 127.0.0.1 50070

pkill $pidnn
pkill $piddn1
pkill $piddn2
