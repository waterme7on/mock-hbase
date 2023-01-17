#!/usr/bin/env bash

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/config.sh
. "$bin"/common.sh


show_usage() {
  echo "Usage: hbase-daemon.sh  (start|stop) <hbase-command> <args...>"
  echo "  hbase-commands:"
  echo "    master"
  echo "  args:"
  echo "    null"
}

if [ "--help" = "$1" ] || [ "-h" = "$1" ]; then
  show_usage
  exit 0
fi


# get arguments
startStop=$1
shift

command=$1
shift

# if no args specified, show usage
if [ ! -n "$command" ]; then
  show_usage
  exit 1
fi



# get log directory
if [ "$HBASE_LOG_DIR" = "" ]; then
  export HBASE_LOG_DIR="$HBASE_HOME/logs"
fi
mkdir -p "$HBASE_LOG_DIR"

# Set default scheduling priority
if [ "$HBASE_NICENESS" = "" ]; then
    export HBASE_NICENESS=0
fi

if [ "$HBASE_PID_DIR" = "" ]; then
  HBASE_PID_DIR=/tmp
fi

if [ "$HBASE_IDENT_STRING" = "" ]; then
  export HBASE_IDENT_STRING="$USER"
fi

export HBASE_LOG_PREFIX=hbase-$HBASE_IDENT_STRING-$command-$HOSTNAME
export HBASE_LOGFILE=$HBASE_LOG_PREFIX.log
HBASE_LOGOUT=${HBASE_LOGOUT:-"$HBASE_LOG_DIR/$HBASE_LOG_PREFIX.out"}
HBASE_LOGGC=${HBASE_LOGGC:-"$HBASE_LOG_DIR/$HBASE_LOG_PREFIX.gc"}
HBASE_LOGLOG=${HBASE_LOGLOG:-"${HBASE_LOG_DIR}/${HBASE_LOGFILE}"}
HBASE_PID=$HBASE_PID_DIR/hbase-$HBASE_IDENT_STRING-$command.pid
export HBASE_ZNODE_FILE=$HBASE_PID_DIR/hbase-$HBASE_IDENT_STRING-$command.znode


cleanAfterRun() {
  if [ -f ${HBASE_PID} ]; then
    # If the process is still running time to tear it down.
    kill -9 `cat ${HBASE_PID}` > /dev/null 2>&1
    rm -f ${HBASE_PID} > /dev/null 2>&1
  fi

  if [ -f ${HBASE_ZNODE_FILE} ]; then
    if [ "$command" = "master" ]; then
      HBASE_OPTS="$HBASE_OPTS $HBASE_MASTER_OPTS" $bin/hbase master clear > /dev/null 2>&1
    else
      #call ZK to delete the node
      ZNODE=`cat ${HBASE_ZNODE_FILE}`
      HBASE_OPTS="$HBASE_OPTS $HBASE_REGIONSERVER_OPTS" $bin/hbase zkcli delete ${ZNODE} > /dev/null 2>&1
    fi
    rm ${HBASE_ZNODE_FILE}
  fi
}

check_before_start(){
    #ckeck if the process is not running
    mkdir -p "$HBASE_PID_DIR"
    if [ -f $HBASE_PID ]; then
      if kill -0 `cat $HBASE_PID` > /dev/null 2>&1; then
        echo $command running as process `cat $HBASE_PID`.  Stop it first.
        exit 1
      fi
    fi
}


thiscmd="$bin/$(basename ${BASH_SOURCE-$0})"

case $startStop in
(start)
    check_before_start
    echo "`date` Running $command" >> $HBASE_LOGLOG
    echo running $command, logging to $HBASE_LOGOUT
    $thiscmd foreground_start $command $args < /dev/null > ${HBASE_LOGOUT} 2>&1  &
    disown -h -r
    sleep 1; head -n 2 "${HBASE_LOGOUT}"
  ;;

(foreground_start)
    trap cleanAfterRun SIGHUP SIGINT SIGTERM EXIT
    echo "`date` Starting $command on `hostname`"
    nice -n $HBASE_NICENESS "$HBASE_HOME"/bin/hbase.sh \
          $command "$@" start &

    hbase_pid=$!
    echo PID: $hbase_pid
    echo $hbase_pid > ${HBASE_PID}
    wait $hbase_pid
  ;;
(stop)
    rm -f "$HBASE_AUTOSTART_FILE"
    echo running $command, logging to $HBASE_LOGOUT
    if [ -f $HBASE_PID ]; then
      pidToKill=`cat $HBASE_PID`
      # kill -0 == see if the PID exists
      if kill -0 $pidToKill > /dev/null 2>&1; then
        echo -n stopping $command
        echo "`date` Terminating $command" >> $HBASE_LOGLOG
        kill $pidToKill > /dev/null 2>&1
        waitForProcessEnd $pidToKill $command
      else
        retval=$?
        echo no $command to stop because kill -0 of pid $pidToKill failed with status $retval
      fi
    else
      echo no $command to stop because no pid file $HBASE_PID
    fi
    rm -f $HBASE_PID
  ;;
(*)
    show_usage
    exit 1
  ;;
esac