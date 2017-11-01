#Check if the service is running before removing it

#Check if the service is running before removing it
PROCESS_NAME=tomcat8
pidfile=${PIDFILE-/opt/apache-tomcat/tomcat8/temp/${PROCESS_NAME}.pid};
PID=`pidofproc -p ${pidfile} ${PROCESS_NAME}`
if [[ (-n ${PID}) && ($PID -gt 0) ]]; then
  service ${PROCESS_NAME} stop
  sleep 1
fi

