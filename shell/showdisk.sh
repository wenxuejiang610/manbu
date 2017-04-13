#!/bin/bash
## useage: check raid info
## Check HP or DELL and Other Server
cat /etc/sinainstall.conf

grep -qi '=hp' /etc/sinainstall.conf
if (($?==0));then 
  SLOT=`sudo hpacucli ctrl all show status|grep Slot|awk '{print $6}'`
  hpacucli controller slot=$SLOT  show status
  echo "HP Raid Level Info:"
  hpacucli controller slot=$SLOT ld all show | awk '{ if ($0 ~ /array/) printf "array "$NF ":"; if($0 ~ /logicaldrive/) print $0}'
  echo '==================================================='
  hpacucli controller slot=$SLOT pd all show | awk '{ if ($0 ~ /array/) printf "array "$NF ":\n"; if($0 ~ /physicaldrive/) print $0}'
else
  Megacli='/opt/MegaRAID/MegaCli/MegaCli64'
  $Megacli -PDList -aALL -NoLog | awk -F":" '{if($1 ~/^Slot/) printf("%s:%2s\t",$1,$2)} ; \
        {if ( $1 ~ /^Media Error/ ) printf("%s:%5s\t",$1,$2)}; \
        {if ( $1 ~ /^Other Error Count/) printf("%s:%5s\t",$1,$2)};\
        {if ( $1 ~ /^Predictive/) printf("%s:%5s\t",$1,$2)}; \
        {if ( $1 ~ /^Firmware state/) printf("%s:%5s\n",$1,$2)}'
  echo -e "\n"
  $Megacli -FwTermLog -Dsply -aALL -NoLog | egrep "CopyBack progress|Rebuild progress" | awk -F"=" '{print $NF}' | tail -1
  $Megacli -LdPdInfo -aALL -Nolog |awk 'BEGIN{print "========================================"}{if($0 ~ /^Virtual Drive:/) print $0};\
        {if($0 ~ /^RAID Level/) print "   "$1,$2,$3,$4,$5}; \
        {if($0 ~ /^Size/) print "     Size: "$3,$4}; \
        {if($0 ~ /^Parity/) print "     Parity Size: "$4,$5}; \
        {if($0 ~ /^Current Cache/) print "     Policy: "$4}; \
        {if($0 ~ /^Enclosure Device ID/) printf("\tEnID:%s",$4)}; \
        {if($0 ~ /^Slot/) printf("  %s",$0)}; \
        {if($0 ~ /^Inquiry Data/) printf("  %s\n",$0)};END{print "========================================"}'
fi
