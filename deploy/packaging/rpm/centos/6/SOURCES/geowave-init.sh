#!/bin/bash
### BEGIN INIT INFO
# Provides:          geowave service
# Required-Start:    $networking
# Required-Stop:     $networking
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Description:       geowave service
# Short-Description: geowave service
### END INIT INFO

. /etc/init.d/functions

PID=
DISPLAY_NAME=geowave
GEOWAVE_HOME=/usr/local/geowave

handle_return_value_and_exit() {
  case $1 in
    0) success ;;
    *) failure ;;
  esac
  echo
  exit $1
}

get_pid() {
  echo $(ps -u geowave | grep -v TTY | awk '{print $1}')
}

is_running() {
  PID=$(get_pid)
  if [ "x$PID" = "x" ]; then return 1; else return 0; fi
}

wait_for_start() {
  MAX_SECONDS_BEFORE_FAIL=15
  timeElapsed=0
  until is_running; do
	  sleep 1
	  let timeElapsed+=1
	  if [ $timeElapsed -gt $MAX_SECONDS_BEFORE_FAIL ]; then
		  return 1
	  fi
  done
  return 0
}

start() {
  echo -n "Starting $DISPLAY_NAME: "
  if ! is_running; then
    if [ -f /etc/geowave/geowave.config ]; then
      SOURCE_CMD="source /etc/geowave/geowave.config;"
    fi
  	runuser -l geowave -c "$SOURCE_CMD $GEOWAVE_HOME/geoserver/bin/startup.sh &" > /dev/null 2>&1
    wait_for_start
    handle_return_value_and_exit $?
  else
    echo -n "Already running, PID $PID"
  fi
  handle_return_value_and_exit 1
}

stop () {
  echo -n "Stopping $DISPLAY_NAME: "

  if is_running; then
    kill_process
    RETVAL=$?
    if [ "$1" != "restart" ]; then
      handle_return_value_and_exit $RETVAL
    else
      if [ $RETVAL -eq 0 ]; then success; else failure; fi
    fi
  else
    if [ "$1" != "restart" ]; then
      echo -n "$DISPLAY_NAME is not running"
      handle_return_value_and_exit 1
    else
      success
    fi
  fi
}

kill_process() {
  MAX_WAIT=10
  PID=$(get_pid)
  runuser -l geowave -c "$GEOWAVE_HOME/geoserver/bin/shutdown.sh"
  RETVAL=$?

  sleep 1
  while is_running; do
    sleep 1
    if [ $MAX_WAIT -le 2 ]; then # Try killing if we get to the end
        if [ "x$PID" != "x" ]; then
            kill $PID
        fi
    fi
    MAX_WAIT=$(expr $MAX_WAIT - 1)
    if [ $MAX_WAIT -le 0 ]; then handle_return_value_and_exit 1; fi
  done
  return $RETVAL
}

status () {
  if is_running; then
    echo "$DISPLAY_NAME is running, PID $PID"
  else
    echo "$DISPLAY_NAME is not running"
    exit 1
  fi
}

case "$1" in
  start) start ;;
  stop)  stop ;;
  restart)
    stop "restart"
    echo
    start
    ;;
  status) status ;;
  *) echo "Usage: $0 {start|stop|restart|status}" ;;
esac
