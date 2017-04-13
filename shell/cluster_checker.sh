#!/bin/bash

#default
maxuse=90
maxavrg=3

#conf dir
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

#log dir
if [ -z $HADOOP_LOG_DIR ]; then
  HADOOP_LOG='/data1/hadoop/logs'
else
  HADOOP_LOG=$HADOOP_LOG_DIR
fi
if [ -z $HBASE_LOG_DIR ]; then
  HBASE_LOG='/data1/hbase/logs'
else
  HBASE_LOG=$HBASE_LOG_DIR
fi

r_cmd='ssh -q -p 26387 -o StrictHostKeyChecking=no -o ConnectTimeout=4'

TOOL_LOG="./cclogs/"
if [ ! -d $TOOL_LOG ]; then
        mkdir -p $TOOL_LOG
else
    rm -rf $TOOL_LOG/*
fi


##find role's ip
master=`/sbin/ifconfig|grep Bcast|awk -F: '{print $2}'|awk '{print $1}'|grep '^10'|sort -nr|head -1`
if [ -d $HBASE_CONF ]; then
  common=$(grep "`cat $HADOOP_CONF/slaves`" $HBASE_CONF/regionservers)
  regionser=$(grep -v "`cat $HADOOP_CONF/slaves`" $HBASE_CONF/regionservers)
  slaves=$(grep -v "`cat $HBASE_CONF/regionservers`" $HADOOP_CONF/slaves)
else
  slaves=$(cat $HADOOP_CONF/slaves)
fi

##find namenode and datanode datadir
namenode=`grep -A1 'dfs.*.name.dir' $HADOOP_CONF_DIR/hdfs-site.xml | egrep -o '/data[0-9]+'|uniq`
datanode=`grep -A1 'dfs.*.data.dir' $HADOOP_CONF_DIR/hdfs-site.xml | egrep -o '/data[0-9]+'|grep -v 'data01'|uniq`

chk_ssh(){
  $r_cmd $1 df -h | grep data > $TOOL_LOG/dfinfo
  if [ $? != 0 ];then
    echo -e "\033[31mSSH Error!\033[0m" && return 255
  fi
}

chk_diskused(){
  ip=$1;node=$2;max=$3
  for dir in $node
  do
  {
    cat $TOOL_LOG/dfinfo | grep $dir &> /dev/null || echo -e "\033[31mDiskLose:$dir\033[0m"
    $r_cmd $ip "touch $dir/hadoop/aaa && rm -f $dir/hadoop/aaa &> /dev/null || echo -e \"\033[31mDiskWrite:$dir\033[0m\""
  } &
  done
  wait
  used=`cat $TOOL_LOG/dfinfo | grep "$node$" | tr -d '%' | awk '{if($5 >= max) printf $6 ":" $5 "%\t"}' max="$max"`
  if [ `echo $used|wc -c` != 1 ]; then
    echo -e "\033[31mDiskused: $used\033[0m"
  fi
}

chk_proc(){
  ip=$1;proc=$2
  $r_cmd $ip ps ax |grep "$proc" &> /dev/null || echo -e "\033[31m$proc is down\033[0m"
}

chk_loadaverage(){
  ip=$1;average=$2
  cmd="$r_cmd $ip \"w|grep 'load average' | awk -F',' '{print \\\$4}'|awk -F':' '{if(\\\$2>"$average") print}'\""
  avrg=`eval $cmd`
  if [ `echo $avrg|wc -c` != 1 ]; then
    echo -e "\033[31m$avrg\033[0m"
  fi
}
chk_common(){
  for i in $common
  do
  {
    echo -e "\033[32m===Common-$i===\033[0m"
    #echo "Active proc:"
    chk_ssh $i || continue
    chk_proc $i $s_proc
    chk_proc $i $r_proc
    chk_diskused $i "$datanode" $maxuse 2> /dev/null
    chk_loadaverage $i $maxavrg
  } &
  wait
  done
}
chk_slaves(){
  for i in $slaves
  do
  {
    echo -e "\033[32m===Slaves-$i===\033[0m"
    chk_ssh $i || continue
    chk_proc $i $s_proc
    chk_proc $i $t_proc
    chk_diskused $i "$datanode" $maxuse 2> /dev/null
    chk_loadaverage $i $maxavrg
  } &
  wait
  done
}
chk_regionser(){
  for i in $regionser
  do
  {
    echo -e "\033[32m===Region-$i===\033[0m"
    chk_ssh $i || continue
    chk_proc $i $r_proc
    chk_diskused $i "$datanode" $maxuse 2> /dev/null
    chk_loadaverage $i $maxavrg
  } &
  wait
  done
}
chk_node(){
  if [ `echo $common|wc -c` != 1 ]; then
    chk_common
  fi
  if [ `echo $slaves|wc -c` != 1 ]; then
    chk_slaves
  fi
  if [ `echo $regionser|wc -c` != 1 ]; then
    chk_regionser
  fi
}

chk_zk(){
  if [ ! -f $TOOL_LOG/zk.host ];then
    hbase org.apache.hadoop.hbase.util.HBaseConfTool hbase.zookeeper.quorum | grep zk | tr ':' ' '| tr ',' '\n' > $TOOL_LOG/zk.host
  fi
  while read zkhost port
  do
    if [ -z $port ];then port=2181;fi
	state=`echo ruok|nc  $zkhost $port`
    if [ -z "$state" ] || [ "imok" != $state ];then
      zkip=`host $zkhost|awk '{print $NF}'`
      zk_err_msg="$zk_err_msg$zkhost($zkip) $port down\n"
    fi
  done < $TOOL_LOG/zk.host

  if [ -z "$zk_err_msg" ];then
    echo -e "\033[33m all zk are ok
==========================\033[0m"
  else
    echo -e "\033[31m$zk_err_msg \033[0m"
  fi
}

hadoop_fsck(){
  echo -e "\033[32m=======Hadoop_fsck========\033[0m"
  hadoop fsck / | tail -n20 > $TOOL_LOG/fsck.log
  if `grep 'HEALTHY' $TOOL_LOG/fsck.log &> /dev/null`;then
    echo "Hadoop fsck ok"
  else
    cat $TOOL_LOG/fsck.log
  fi
}
hbase_hbck1(){
  echo -e "\033[32m========HBase_fsck========\033[0m"
  hbase hbck | tail -n100 > $TOOL_LOG/hbck.log
  if `grep 'OK' $TOOL_LOG/hbck.log &> /dev/null`;then
    echo "HBase hbck ok"
  else
    cat $TOOL_LOG/hbck.log
  fi
}

hbase_hbck(){
  echo -e "\033[32m========HBase_fsck========\033[0m"
  hbase hbck > $TOOL_LOG/hbck.log
  if `grep '!OK' $TOOL_LOG/hbck.log &> /dev/null`;then
    echo "HBase hbck ok"
  else
        deal_error_log $TOOL_LOG/hbck.log $TOOL_LOG;
    #cat $TOOL_LOG/hbck.log
  fi
}

statics(){
  ip=$1
  metrics=$2
  echo -e "\033[32m========$ip:$metrics========\033[0m"
  curl -s http://$ip:60030/metrics |sed 's/tbl.//g'|grep "$metrics"  |awk -F '.region.|=' '{print $1,$2,$3|"sort -k3nr|head -n5"}'
}

find_hot_regions(){
  for regionserver in `cat $HBASE_CONF/regionservers`
  do
    statics $regionserver $1
  done
}

print_classify_log(){
classify_log=$1
filelist=`ls $classify_log`
flag_deployed="!isDeployed"
flag_meta="inMeta"
for file in $filelist;do
        echo "$file" |grep -q "$flag_meta"
        if [ $? -eq 0 ];then
                echo "$file" |grep -q "$flag_deployed"
                        if [ $? -eq 0 ];then
                                echo -e "\033[32m======="$file"=========\033[0m"
                                cat $classify_log/$file | awk -F"meta =>|, hdfs" '{print $2}'
                        else
                                echo -e "\033[32m======="$file"=========\033[0m"
                                cat $classify_log/$file | awk -F"meta =>| hdfs" '{print $2$3$4}' | awk -F" =>| }" '{print $1$4}'
                        fi
        elif [ "$file" = "deadserver1" ];then
            if !`grep ': 0' $classify_log/$file &> /dev/null`;then
                        echo ""
                #else
                        #echo -e "\033[32m======="$file"=========\033[0m"
                        #cat $classify_log/$file
                fi
        else
                echo -e "\033[32m======="$file"=========\033[0m"
                cat $classify_log/$file
        fi
done
}

deal_error_log(){
  hbck_log=$1
  classify_log=$2/cl/
  if [ ! -d $classify_log ]; then
        mkdir -p $classify_log
  fi
awk -v dir="$classify_log/" '{
errorcase["root1"]="Region or some of its attributes are null"
errorcase["root2"]="Fatal error: unable to get root region location. Exiting..."
errorcase["meta1"]=".META. is not found on any region."
errorcase["meta2"]="Two entries in META are same"
errorcase["meta3"]=".META. is found on more than one region."
errorcase["meta4"]="Encountered fatal error. Exiting..."
errorcase["deadserver1"]="Number of dead region servers:"
errorcase["deadserver2"]="Unable to fetch region information."
errorcase["hdfs1"]="Version file does not exist in root dir"
errorcase["hdfs2"]="duplicate??"
errorcase["hdfs3"]="Orphan region in HDFS: Unable to load .regioninfo from table"
errorcase["hdfs4"]="Unable to read .tableinfo from"
errorcase["hdfs5"]="ERROR: Found lingering reference file"
errorcase["!inMeta&&!inHdfs&&isDeployed"]=", not on HDFS or in META but "
errorcase["!inMeta&&inHdfs&&!isDeployed"]="on HDFS, but not listed in META "
errorcase["!inMeta&&inHdfs&&isDeployed  "]="not in META, but deployed on"
errorcase["inMeta&&inHdfs&&!isDeployed&&splitParent "]="is a split parent in META, in HDFS,"
errorcase["inMeta&&!inHdfs&&!isDeployed"]="found in META, but not in HDFS"
errorcase["inMeta&&!inHdfs&&isDeployed"]="found in META, but not in HDFS, "
errorcase["inMeta&&inHdfs&&!isDeployed&&shouldBeDeployed"]="not deployed on any region server."
errorcase["inMeta&&inHdfs&&isDeployed&&!shouldBeDeploye"]="should not be deployed according"
errorcase["inMeta&&inHdfs&&isMultiplyDeployed"]="but is multiply assigned to region servers "
errorcase["inMeta&&inHdfs&&isDeployed&&!deploymentMatchesMeta"]="but found on region server "
errorcase["unforeseen"]="is in an unforeseen state"
for(i in errorcase){
if($0~errorcase[i]){print >>dir""i};
}
}' $hbck_log
  print_classify_log $classify_log
}

analysis_log(){
  features=$1
  if [ -z $log_type ]; then
    usage
    exit 1
  else
    case $log_type in
      dfs)
      analysis_dfs_log "$features"
      ;;
      mr)
      analysis_mr_log "$features"
      ;;
      hb)
      analysis_hb_log "$features"
      ;;
      *)
      echo "input error! please check your input."; usage ; exit 2
      ;;
    esac
  fi
}

analysis_dfs_log(){
  str=$1
  echo $log_date
  echo -e "\033[32m===Namenode===\033[0m"
  cmd="cat $HADOOP_LOG/hadoop-hadoop-namenode-*.log | $str"
  if [ ! -z $log_date ];then
	cmd="cat $HADOOP_LOG/hadoop-hadoop-namenode-*.log*$log_date* | $str"
  fi
  eval $cmd
 for ip in `cat $HADOOP_CONF/slaves`
  do
  {
    echo -e "\033[32m===Datanode-$ip===\033[0m"
	if [ ! -z $log_date ];then
		$r_cmd $ip "cat $HADOOP_LOG/hadoop-hadoop-datanode-*.log*$log_date* |$str" || continue
	else
	    $r_cmd $ip "cat $HADOOP_LOG/hadoop-hadoop-datanode-*.log |$str" || continue
	fi

  } &
  wait
  done
}

analysis_mr_log(){
  str=$1
  echo -e "\033[32m===Jobtracker===\033[0m"
  cmd="cat $HADOOP_LOG/hadoop-hadoop-jobtracker-*.log*$log_date*| $str"
  eval $cmd
  for ip in `cat $HADOOP_CONF/slaves`
  do
  {
    echo -e "\033[32m===Tasktracker-$ip===\033[0m"
	if [ ! -z $log_date ];then
		$r_cmd $ip "cat $HADOOP_LOG/hadoop-hadoop-tasktracker-*.log*$log_date* |$str" || continue
	else
	    $r_cmd $ip "cat $HADOOP_LOG/hadoop-hadoop-tasktracker-*.log |$str" || continue
	fi
  } &
  wait
  done
}

analysis_hb_log(){
  str=$1
  echo -e "\033[32m===HMaster===\033[0m"
  cmd="cat $HBASE_LOG/hbase-hadoop-master-*.log*$log_date* | $str"
  eval $cmd
  for ip in `cat $HBASE_CONF/regionservers`
  do
  {
    echo -e "\033[32m===Datanode-$ip===\033[0m"
	if [ ! -z $log_date ];then
		$r_cmd $ip "cat $HBASE_LOG/hbase-hadoop-regionserver-*.log*$log_date* |$str" || continue
	else
	    $r_cmd $ip "cat $HBASE_LOG/hbase-hadoop-regionserver-*.log |$str" || continue
	fi
   } &
  wait
  done
}

chk_master(){
  echo -e "\033[32m===Master-$master===\033[0m"
  echo "Active proc:"
  jps|grep -v Jps| awk '{print $NF}' | sort
  chk_ssh $master
  chk_diskused $master "$namenode" $maxuse
  echo -e "\033[33m==========================\n\
  $(curl -s http://$master:50070/jmx?qry=hadoop:service=NameNode,name=NameNodeInfo | egrep 'PercentUsed|Safemode' | tr -d ',"'|tr -s '  ')\
  \033[0m"
}

gcdetail(){
  for ip in `cat $HBASE_CONF/regionservers`
  do
  {
    echo -e "\033[32m===Region-$ip===\033[0m"
    $r_cmd $ip 'jstat -gcutil `jps|grep HRegionServer|awk '\''{print $1}'\''`'
  } &
  wait
  done
}

usage(){
  echo "usage: cluster_checker.sh [-m maxuse (defalt 90)] -c[b][f]"
  echo ""
  echo "example: cluster_checker.sh -c   #check master,zk and nodes"
  echo "example: cluster_checker.sh -b   #hbase hbck"
  echo "example: cluster_checker.sh -f   #hadoop fsck"
  echo "example: cluster_checker.sh -cb  #check master,zk,nodes and hbase_hbck"
  echo "example: cluster_checker.sh -cf  #check master,zk,nodes and hadoop_fsck"
  echo "example: cluster_checker.sh -u 90 -a 3 -c  #set maxuse 90%, maxavrg 3 and check master,zk,node"
  echo "example: cluster_checker.sh -t <dfs,mr,hb> -d 2015-01-[0][4] -l \"grep test| ... \" #all node log analysis"
  echo "example: cluster_checker.sh -h   #find hot read/write regions （support 0.94 only）"
  echo "example: cluster_checker.sh -g   #print gc detail"
}

#cmd_r='jps'
#cmd_d='ps ax'
r_proc='regionserver'
s_proc='datanode'
t_proc='tasktracker'

if [ $# -lt 1 ] ;
    then
         usage
         exit 1
else
while getopts cfbhgl:t:a:u:d: OPTION
do
  case $OPTION in
    u)
    maxuse=$OPTARG
    ;;
    a)
    maxavrg=$OPTARG
    ;;
    c)
    { chk_master;chk_zk;chk_node;} 2> /dev/null
    ;;
    b)
    hbase_hbck 2> /dev/null
    ;;
    h)
    find_hot_regions readrequestcount
    find_hot_regions writerequestcount
    ;;
    f)
    hadoop_fsck 2> /dev/null
    ;;
    l)
    analysis_log "$OPTARG"
    ;;
    t)
    log_type=$OPTARG
    ;;
    d)
    log_date=$OPTARG
    ;;
    g)
    gcdetail
    ;;
    *)
    echo "input error! please check your input."; usage ; exit 2
    ;;
  esac
done
fi
