#Check if the service is running before removing it
PROCESS_NAME=gwtomcat
pidfile=${PIDFILE-/var/run/${PROCESS_NAME}.pid};
PID=$(cat ${pidfile})
if [[ (-n ${PID}) && ($PID -gt 0) ]]; then
  service ${PROCESS_NAME} stop
  sleep 1
fi

