#!/usr/bin/env bash

if [ $# -lt 4 ];then
 echo "usage : sh backup.sh \"tablename1,tablename2,tablename3,...\" \"sourceIP\" \"dstIP\" \"clusterName\""
 echo "example : sh backup.sh \"tb-user-tag,tb-tag-dic,tb-phone-user,tb-user-tag-old,tb-tag-dic-old,tb-phone-user-old\" \"10.75.17.175\" \"10.75.6.75\" \"wbdp\""
 exit 0
fi

tableNames=$(echo $1 | awk -F ',' '{for (i = 0; i<NF; i++) print $((i+1))}')
sourceIP=$2
dstIP=$3
cluster=$4
today=`date +%Y%m%d`

for tableName in ${tableNames[*]};do
ssh -T -p 26387 -i ~/.ssh/id_rsa2 -o StrictHostKeyChecking=no root@${sourceIP} << EOF
su - hadoop
echo "snapshot '${tableName}','${tableName}-snapshot'" | hbase shell
hbase org.apache.hadoop.hbase.snapshot.ExportSnapshot -snapshot ${tableName}-snapshot -copy-to hdfs://${dstIP}:9000/backup/${cluster}/${today} -mappers 16
echo "delete_snapshot '${tableName}-snapshot'" | hbase shell
EOF
done
