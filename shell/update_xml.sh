#!/bin/bash
file=$1
pram=$2
value=$3

usage(){
  echo "usage: update_xml.sh  file pram value"
}

difffun(){
	diff $1 $2
}

if [ $# -lt 3 ] ;then
    usage
    exit 1
fi

if [ ! -f $file ]; then
	echo "File "$file" does not exist";
	exit 1
fi

file_bak=$file".bak_"`date +%Y%m%d`
if [ -f $file_bak ]; then
	mv $file_bak $file_bak"_"`date +%s`
fi
cp $file $file_bak

cat $file_bak |grep -q "$pram"
if [ $? -eq 0 ];then
sed '/'"$pram"'/,/value/c\
    <name>'"$pram"'</name>\
    <value>'"$value"'</value>' $file_bak > $file
echo "======update======"
difffun $file $file_bak
else
sed '/\/configuration/ i\
  <property>\
    <name>'"$pram"'</name>\
    <value>'"$value"'</value>\
  </property>\
' $file_bak > $file
echo "======insert======"
difffun $file $file_bak
fi 
