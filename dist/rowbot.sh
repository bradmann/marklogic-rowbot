#!/bin/bash

PIDFILE=/root/rowbot.pid
if [ -f $PIDFILE ]
then
  PID=$(cat $PIDFILE)
  ps -p $PID > /dev/null 2>&1
  if [ $? -eq 0 ]
  then
    echo "Process already running"
    exit 1
  else
    ## Process not found assume not running
    echo $$ > $PIDFILE
    if [ $? -ne 0 ]
    then
      echo "Could not create PID file"
      exit 1
    fi
  fi
else
  echo $$ > $PIDFILE
  if [ $? -ne 0 ]
  then
    echo "Could not create PID file"
    exit 1
  fi
fi

jobfile=$1
if [ $# -ne 1 ]; then
	echo $0: "You must specify a job file!"
	rm $PIDFILE
	exit 1
fi

java -jar rowbot-all.jar $jobfile

rm $PIDFILE