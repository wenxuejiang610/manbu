#!/bin/sh
#HBase's regionserver rolling graceful restart.

function usage(){
 echo "Usage: rs_restrt.sh (restart|checkStata) ['ip1,ip2']"
 exit 1
}

#sort ip and make md5
#para1:ip list
function str2md5(){
 echo $1 | tr "," "\n" | sort | tr "\n" "," | md5sum | tr " -" "m"
}

#rolling graceful restart regionserver
#para1:ip list; para2:log's path; para3:graceful log; para4:current ip log
function rolling_graceful(){
 ipsList=`echo $1 | tr "," "\n" | sort | tr "\n" " "`
 idx=0
 for ip in $ipsList; do
 idx=`expr $idx + 1`
 echo $idx":"$ip>>$2/$4
 su - hadoop -c "graceful_stop.sh --restart --reload $ip &> $2/$3"
 #sudo -u hadoop -c graceful_stop.sh --restart --reload $ip &> $2/$3
 done
}

if [ $# -lt 2 ]; then
 usage
fi

command=$1
ips=$2
ipsmd5=`str2md5 $ips`
path="/data1/hbase/logs/gracefulrestart/"$ipsmd5
logFile="graceful.log"
currentIpFile="currentip.log"

case $command in
 (restart)
 if [ -n "$ips" ];then
 su - hadoop -c "mkdir -p $path"
 if [ -w $path ];then
 rolling_graceful $ips $path $logFile $currentIpFile
 fi
 fi
 ;;
 (checkStata)
 #cat $path/$currentIpFile
 #tail -1 $path/$logFile
 currentip=`tail -1 $path/$currentIpFile`
 currentip=${currentip:2}
 taillog=`tail -1 $path/$logFile`

 echo $taillog

 echo "$taillog" |grep -q "(" && !(echo "$taillog" |grep -q $currentip)
 if [ $? -eq 0 ];then
 echo $currentip: regions move out.
 fi

 echo "$taillog" |grep -q "(" && echo "$taillog" |grep -q $currentip
 if [ $? -eq 0 ];then
 echo $currentip: regions move in.
 fi

 echo "$taillog" |grep -q "Restoring balancer state to"
 if [ $? -eq 0 ];then
 echo $currentip: graceful restart is ok.
 fi
 ;;
 (*)
 echo $usage
 exit 1
 ;;
esac
