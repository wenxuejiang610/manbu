#!/bin/bash
source $HADOOP_CONF_DIR/hadoop-env.sh
blackList=$(hadoop job -list-blacklisted-trackers|sed 's/tracker_//'|sed 's/:.*//')
for hostname in ${blackList[*]}
do
        echo "$hostname will be restarting..."
        pid=$(ssh $HADOOP_SSH_OPTS $hostname ps -ef |grep -v grep|grep tasktracker|awk '{print $2}')
        ssh $HADOOP_SSH_OPTS $hostname kill -9 $pid
        sleep 3
        ssh $HADOOP_SSH_OPTS $hostname hadoop-daemon.sh start tasktracker
done