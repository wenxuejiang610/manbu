#!/bin/bash
## use to make raid0

chkbin='/opt/MegaRAID/MegaCli/MegaCli64'
echo '1.clear foreign disk:'
$chkbin -CfgForeign -Clear -a0 -NOLOG &> ./Autoraid0.log
if [ $? == 0 ] ;then
  echo '> clear foreign disk ok'
else
  echo '>>clear foreign disk fail' && exit 255
fi

echo '2.Input error disk info:'
read -p 'Input Disk Id(0-11):' id
read -p 'Input Target Id(0-11):' vid
read -p 'Input Mount Dir(/data*):' dir
eid=`$chkbin -pdlist -a0 -NoLOG|grep -B1 "Slot Number: $id"|head -n1|awk '{print $NF}'`
echo "> eid: $eid"
echo "> id: $id"
echo "> Target Id: $vid"
echo "> Mount Dir: $dir"
read -p 'check error disk info (input y/n):' ans
if [ $ans == 'y' ] ;then
  
  echo "3.clear vd$vid cache:"
  $chkbin -DiscardPreservedCache -L${vid} -a0 -NoLOG >> ./Autoraid0.log
  if [ $? == 0 ] ;then
    echo "> clear cache ok "
  else
    echo ">>clear cache fail" && exit 255
  fi
  
  echo "4.make raid0:"
  $chkbin -CfgLdAdd -r0 [$eid:$id] WB -a0 -NOLOG >> ./Autoraid0.log
  if [ $? == 0 ] ;then
    echo "> make raid0 ok "
  else
    echo ">>make raid0 fail" && exit 255
  fi
  
  echo "5.mkfs and mount:"
  if `df |grep $dir &> /dev/null`;then 
    umount -l $dir || exit 255
  fi
  olddev=`df | grep -o "/dev/sd[a-z]"| sort | uniq | xargs`
  fstype=`df -T|grep data|tail -n1|awk '{print $2}'`
  for i in `ls /dev/sd?`
  do
    echo $olddev|grep "$i" &> /dev/null
    if [ $? != 0 ] ;then
      newdev=$i && break
    fi
  done
  if [ -z $newdev ] ;then
    echo "newdisk is null" && exit 3
  fi
  echo "> newdisk is $newdev"
  echo "> check online mount .."
  re=`mount | grep $newdev`
  if [ -z $re ] ;then
    echo "> check result is ok"
    echo "parted -s $newdev mklabel gpt"
    parted -s $newdev mklabel gpt
    echo "parted -s $newdev mkpart primary $fstype 0 100%"
    parted -s $newdev mkpart primary $fstype 0 100%
    mkfs.$fstype -L $dir ${newdev}1
    
    sleep 3
    
    sed -i "/\\$dir/d" /etc/fstab
    uuid=`blkid |grep $newdev|tr -d '"'|awk -F "UUID=" '{print $2}'|awk '{print $1}'`
    echo "UUID=$uuid  $dir                  $fstype    defaults        0 0" >> /etc/fstab
    mount -a && mkdir -p $dir/hadoop && chown hadoop:hadoop -R $dir/hadoop
    #restart datanode
    su -l hadoop -c 'hadoop-daemon.sh stop datanode';sleep 1; su -l hadoop -c 'hadoop-daemon.sh start datanode'
  else
    echo ">>dev is online!!" && exit 2
  fi

else
  echo '>> exit' && exit 1 
fi
