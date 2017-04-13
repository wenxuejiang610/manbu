#!/bin/bash
usage()
{
    echo "usage: tcollectorctr.sh -h <host> -a <action>[start|stop|install] -c <clustertypeid>";
    echo ""
    echo "example: tcollectorctr.sh -h 10.0.0.1 -a start -c weibo ";
    echo "example: tcollectorctr.sh -h 10.0.0.1 10.0.0.2 -a stop ";
}

if [ $# -lt 6 ] ;then
    usage
    exit 1
else
  while getopts h:a:c: OPTION
  do
     case "$OPTION"  in
       h)ip=$OPTARG;;
       a)action=$OPTARG;;
       c)clustertypeID=$OPTARG;;
       *) echo -e "\033[31minput error! please check your input\033[0m"; usage ; exit 2; ;;
     esac
  done
fi

for host in $ip
do
{
    if [ $action == 'stop' ]; then
        curl -ks 'http://api.dsp.cluster.sina.com.cn/tcollector/service/' -d "tgt=$host&action=$action";echo
    else
        #check clustertypeid
        if [ $clustertypeID == 'clustertypeid' ];then
            echo -e '\033[31merror,check the clustertypeID!\033[0m' && exit 1
        fi

        if [ $action == 'install' ]; then
            curl -ks 'http://api.dsp.cluster.sina.com.cn/tcollector/service/' -d "tgt=$host&action=$action";echo
        fi
        curl -ks 'http://api.dsp.cluster.sina.com.cn/tcollector/instance/' -d "tgt=$host&cname=$clustertypeID&action=start";echo
    fi
} &
done
wait
