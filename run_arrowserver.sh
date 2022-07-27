#!/bin/bash

cd "$(dirname "$0")"

./gradlew clean
./gradlew build
./gradlew shadowJar #--warning-mode all --stacktrace

java -jar build/libs/lazo-0.1.0-all.jar --add-opens=java.base/java.nio=ALL-UNNAMED &
server_pid=$!
echo $server_pid > /tank/local/suhail/relic-datalake/scripts/server.pid
#java -jar build/libs/lazo-0.1.0-uber.jar --add-opens=java.base/java.nio=ALL-UNNAMED