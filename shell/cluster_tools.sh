#!/bin/bash

scp_cmd='scp -P26387 -o StrictHostKeyChecking=no'
ssh_cmd='ssh -p 26387 -o StrictHostKeyChecking=no'

if [ -z $HADOOP_CONF_DIR ]; then
  HADOOP_CONF='/usr/local/common/hadoop/conf'
else
  HADOOP_CONF=$HADOOP_CONF_DIR
fi
if [ -z $HBASE_CONF_DIR ]; then
  HBASE_CONF='/usr/local/common/hbase/conf'
else
  HBASE_CONF=$HBASE_CONF_DIR
fi

regionser=`cat $HBASE_CONF/regionservers`
slaves=`cat $HADOOP_CONF/slaves`

usage(){
  echo ""
  echo "example: cluster_tools.sh -t <hadoop,hbase> -u filename" #sync config file "
  echo "example: cluster_tools.sh -t <hadoop,hbase> -r" #rolling restart datanode or regionserver "
}

restart_server(){
  if [ -z $type ]; then
    usage
    exit 1
  else
    case $type in
      hadoop)
      restart_datanodes
      ;;
      hbase)
      restart_regionservers
      ;;
      *)
      echo "input error! please check your input."; usage ; exit 2
      ;;
    esac
  fi
}

restart_datanodes(){
  for ip in $slaves
    do
    echo $ip
    date
    $ssh_cmd $ip hadoop-daemon.sh stop datanode
    sleep 5
    $ssh_cmd $ip hadoop-daemon.sh start datanode
    sleep 30
  done
}

restart_regionservers(){
  zparent=`$HBASE_HOME/bin/hbase org.apache.hadoop.hbase.util.HBaseConfTool zookeeper.znode.parent`
  if [ "$zparent" == "null" ]; then
    zparent="/hbase"
  fi
  online_regionservers=`$HBASE_HOME/bin/hbase zkcli ls $zparent/rs 2>&1 | tail -1 | sed "s/\[//" | sed "s/\]//"`
  for regionservername in $online_regionservers
    do
    ip=`echo $regionservername|awk -F',' '{print $1}'`
    echo $ip
    date
    graceful_stop.sh --restart --reload $ip
  done
}

update_file(){
  file=$1
  bakfilename=".bak_"`date +%Y%m%d%H%M`
  if [ -z $type ]; then
    usage
    exit 1
  else
    case $type in
      hadoop)
      iplist=$slaves
      dir=$HADOOP_CONF
      ;;
      hbase)
      iplist=$regionser
      dir=$HBASE_CONF
      ;;
      *)
      echo "input error! please check your input."; usage ; exit 2
      ;;
    esac
  fi
  echo "starting copyfile:$file to cluster..."
  for ip in $iplist
  do
    echo "$scp_cmd $dir/$file $ip:$dir/$file"
    $ssh_cmd $ip mv $dir/$file $dir/$file$bakfilename
    $scp_cmd $dir/$file $ip:$dir/$file
  done
}

if [ $# -lt 1 ] ;
    then
         usage
         exit 1
else
while getopts u:rt: OPTION
do
  case $OPTION in
    t)
    type=$OPTARG
    ;;
    u)
    update_file $OPTARG
    ;;
    r)
    restart_server >restart.log 2>&1 &
    echo "log in `pwd`/restart.log"
    ;;
    *)
    echo "input error! please check your input."; usage ; exit 2
    ;;
  esac
done
fi