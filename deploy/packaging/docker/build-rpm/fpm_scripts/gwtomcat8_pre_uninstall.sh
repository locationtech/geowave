#Check if the service is running before removing it
PROCESS_NAME=gwtomcat8
pidfile=${PIDFILE-/usr/local/geowave/tomcat8/temp/${PROCESS_NAME}.pid}
PID=`pidofproc -p ${pidfile} ${PROCESS_NAME}`
if [[ (-n ${PID}) && ($PID -gt 0) ]]; then
  systemctl stop ${PROCESS_NAME}
  sleep 1
fi

