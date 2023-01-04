bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

# This will set HBASE_HOME, etc.
. "$bin"/config.sh


JAVA=$JAVA_HOME/bin/java

java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug -jar hbase-server/target/hbase-server-1.0.jar start --masters=1