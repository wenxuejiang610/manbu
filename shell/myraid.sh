#/bin/bash
#2014-11-12
#gaopeng4

vendor=`dmidecode | grep -i 'Vendor' --color`
echo $vendor
product=`dmidecode | grep "Product Name" --color | head -n 1`
echo $product

sma()
{
echo "Cache Ratio:"
hpacucli controller all show detail config | grep 'Cache Ratio' | awk -F: '{print $2}' | tr -d ' '
echo '-------------------'
echo "Battery Status:"
hpacucli controller all show detail config | grep 'Battery/Capacitor Status' | awk -F: '{print $2}' | tr -d ' '
echo '-------------------'

i=0; 
while read var; 
do 
A[$i]=$var;
let i+=1; 
done<<EOF
`hpacucli controller all show config | grep 'array'`
EOF
A[$i]='Enclosure SEP'
for((j=0;j<$i;j++))
do
aa=${A[$j]}
bb=${A[$((j+1))]}
hpacucli controller all show config |  sed -n "/$aa/,/$bb/p" | head -n -2 | sed '/^$/d'
echo $raid
echo '------' 
done
}

lsi()
{
i=0; 
while read var; 
do 
A[$i]=$var;
let i+=1; 
done<<EOF
`/opt/MegaRAID/MegaCli/MegaCli64  -ldpdinfo -aall  | grep 'Virtual Drive:' | awk -F'(' '{print $1}' `
EOF
A[$i]='Exit Code: 0x00'

for((j=0;j<$i;j++))
do
aa=${A[$j]}
bb=${A[$((j+1))]}

raid_level=`/opt/MegaRAID/MegaCli/MegaCli64  -ldpdinfo -aall | sed -n "/$aa/,/$bb/p" | head -n -2 | sed '/^$/d' | egrep 'RAID Level' | awk -F: '{print $2}' | awk -F, '{print $1","$2}' | tr -d ' '`

raid_size=`/opt/MegaRAID/MegaCli/MegaCli64  -ldpdinfo -aall | sed -n "/$aa/,/$bb/p" | head -n -2 | sed '/^$/d' | egrep '^Size' | awk -F: '{print $2}' | tr -d ' '`

raid_policy=`/opt/MegaRAID/MegaCli/MegaCli64  -ldpdinfo -aall | sed -n "/$aa/,/$bb/p" | head -n -2 | sed '/^$/d' | grep 'Default Cache Policy' | awk -F: '{print $2}' | tr -d ' '`

spans=`/opt/MegaRAID/MegaCli/MegaCli64  -ldpdinfo -aall | sed -n "/$aa/,/$bb/p" | head -n -2 | sed '/^$/d'  | egrep 'Number of Spans' | awk -F: '{print $2}' | tr -d ' ' `

if [[ $spans = 1 ]]
then
pd_per_span=`/opt/MegaRAID/MegaCli/MegaCli64  -ldpdinfo -aall | sed -n "/$aa/,/$bb/p" | head -n -2 | sed '/^$/d' | egrep 'Raw Size|Media Error Count|Firmware state|Media Type|Device Id:' | awk -F: '{print $2}' | tr -d ' ' | awk -F[ '{print $1}'  | xargs -n 5 | wc -l`
else
pd_per_span=`/opt/MegaRAID/MegaCli/MegaCli64  -ldpdinfo -aall | sed -n "/$aa/,/$bb/p" | head -n -2 | sed '/^$/d'  | egrep 'Number Of Drives per span' | awk -F: '{print $2}' | tr -d ' ' `
fi

echo -e "raid_level:$raid_level\traid_size:$raid_size\tspans:$spans\tpd_per_span:$pd_per_span"
echo "raid_policy:$raid_policy"

echo -e "Id\tSize\t\tError\tState\t\tType "
/opt/MegaRAID/MegaCli/MegaCli64  -ldpdinfo -aall | sed -n "/$aa/,/$bb/p" | head -n -2 | sed '/^$/d' | egrep 'Raw Size|Media Error Count|Firmware state|Media Type|Device Id:' | awk -F: '{print $2}' | tr -d ' ' | awk -F[ '{print $1}'  | xargs -n 5 | awk '{print $1"\t"$3"\t"$2"\t"$4"\t"$5}'

echo '---------------------'
done
}



my_raid()
{
raid_ctrl=`lspci | grep -i "raid" --color`
if [[ $raid_ctrl == *LSI*Logic* ]]  
then
echo "Controller:LSI"
lsi
else
    #Hewlett-Packard Company Smart Array
    if [[ $raid_ctrl == *Hewlett-Packard*Company* ]]
    then 
    echo "Controller:Smart Array Controller"
    sma
    else
    echo "Unknow Raid Controller!!!!!"
    fi
fi
}

my_raid

