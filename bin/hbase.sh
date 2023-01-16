#! /usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

# in_dev_env=1

# This will set HBASE_HOME, etc.
. "$bin"/config.sh

show_usage() {
    echo "Usage: hbase.sh <commands> [<args>]"
    echo "Commands:"
    echo "  master         run a HBase master node"
}

if [ "--help" = "$1" ] || [ "-h" = "$1" ]; then
  show_usage
  exit 0
fi

# if no args specified, show usage
if [ $# = 0 ]; then
  show_usage
  exit 1
fi



JAVA=$JAVA_HOME/bin/java

# get arguments
COMMAND=$1
shift


# establish a default value for HBASE_OPTS if it's not already set. For now,
# all we set is the garbage collector.
if [ -z "${HBASE_OPTS}" ] ; then
  major_version_number="$(parse_java_major_version "$(read_java_version)")"
  case "$major_version_number" in
  8|9|10)
    HBASE_OPTS="-XX:+UseConcMarkSweepGC"
    ;;
  11|*)
    HBASE_OPTS="-XX:+UseG1GC"
    ;;
  esac
  export HBASE_OPTS
fi


if [ "$COMMAND" = "master" ] ; then
  CLASS='org.waterme7on.hbase.master.HMaster'
else
  show_usage
  exit 1
fi

# CLASSPATH initially contains $HBASE_CONF_DIR
CLASSPATH="${HBASE_CONF_DIR}"
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar


add_to_cp_if_exists() {
  if [ -d "$@" ]; then
    CLASSPATH=${CLASSPATH}:"$@"
  fi
}

add_jar_under_path() {
  if [ -d "$@" ]; then
    for f in "$@"/*.jar; do
      CLASSPATH=${CLASSPATH}:$f;
    done
  fi
}

add_to_cp_if_exists "${HBASE_HOME}/hbase-server/target"
# Needed for GetJavaProperty check below
add_to_cp_if_exists "${HBASE_HOME}/hbase-server/target/classes"
add_jar_under_path "${HBASE_HOME}/hbase-server/target/lib"



# Add user-specified CLASSPATH last
if [ "$HBASE_CLASSPATH" != "" ]; then
  CLASSPATH=${CLASSPATH}:${HBASE_CLASSPATH}
fi
# Add user-specified CLASSPATH prefix first
if [ "$HBASE_CLASSPATH_PREFIX" != "" ]; then
  CLASSPATH=${HBASE_CLASSPATH_PREFIX}:${CLASSPATH}
fi



#If avail, add Hadoop to the CLASSPATH and to the JAVA_LIBRARY_PATH
# Allow this functionality to be disabled
if [ "$HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP" != "true" ] ; then
  HADOOP_IN_PATH=$(PATH="${HADOOP_HOME:-${HADOOP_PREFIX}}/bin:$PATH" which hadoop 2>/dev/null)
fi


###########################################################################

# Exec unless HBASE_NOEXEC is set.
export CLASSPATH


# resolve the command arguments
CMD_ARGS=("$@")
if [ "${#JSHELL_ARGS[@]}" -gt 0 ] ; then
  CMD_ARGS=("${JSHELL_ARGS[@]}" "${CMD_ARGS[@]}")
fi

export JVM_PID="$$"
exec "$JAVA" -Dproc_$COMMAND -XX:OnOutOfMemoryError="kill -9 %p" -Dorg.slf4j.simpleLogger.defaultLogLevel=debug -cp $CLASSPATH $HBASE_OPTS $CLASS "${CMD_ARGS[@]}"
