磁盘检查：
curl -s http://dspgit.cluster.sina.com.cn/bigdata_group/toolbox/raw/master/shell/showdisk.sh|bash

一键巡检：
curl -s "http://dspgit.cluster.sina.com.cn/bigdata_group/toolbox/raw/master/shell/cluster_checker.sh"|bash /dev/stdin -u 90 -a 3 -c

读写排名TOP5：
curl -s "http://dspgit.cluster.sina.com.cn/bigdata_group/toolbox/raw/master/shell/cluster_checker.sh"|bash /dev/stdin -h

日志分析：
curl -s "http://dspgit.cluster.sina.com.cn/bigdata_group/toolbox/raw/master/shell/cluster_checker.sh"|bash /dev/stdin -t hb -l "grep "

GC状态查看：
curl -s "http://dspgit.cluster.sina.com.cn/bigdata_group/toolbox/raw/master/shell/cluster_checker.sh"|bash /dev/stdin -g

黑名单移除：
curl -s "http://dspgit.cluster.sina.com.cn/bigdata_group/toolbox/raw/master/shell/remove_blacklist_tracker.sh"|bash

同步配置文件：
curl -s "http://dspgit.cluster.sina.com.cn/bigdata_group/toolbox/raw/master/shell/cluster_tools.sh"|bash /dev/stdin -t hbase -u hbase-site.xml

滚动重启hbase
curl -s "http://dspgit.cluster.sina.com.cn/bigdata_group/toolbox/raw/master/shell/cluster_tools.sh"|bash /dev/stdin -t hbase -r

滚动重启datanode
curl -s "http://dspgit.cluster.sina.com.cn/bigdata_group/toolbox/raw/master/shell/cluster_tools.sh"|bash /dev/stdin -t hadoop -r