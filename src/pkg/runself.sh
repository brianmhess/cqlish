#!/bin/bash
MYSELF=`which "$0" 2>/dev/null`
[ $? -gt 0 -a -f "$0" ] && MYSELF="./$0"
java=java
if test -n "$JAVA_HOME"; then
    java="$JAVA_HOME/bin/java"
fi
#mkdir -p target/sigar
#unzip -j -n -d target/sigar $MYSELF "libs/*" > /dev/null 2>&1
#exec "$java" -Djava.library.path=target/sigar -Dcassandra.jmx.local.port=7199 -XX:+UseG1GC -Xmx4G -Xms4G -XX:+UseTLAB -XX:+ResizeTLAB $java_args -jar $MYSELF "$@"
exec "$java" -Dcassandra.jmx.local.port=7199 -XX:+UseG1GC -Xmx4G -Xms4G -XX:+UseTLAB -XX:+ResizeTLAB $java_args -jar $MYSELF "$@"
exit 1
