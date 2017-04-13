# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# Set environment variables here.

# The java implementation to use.  Java 1.6 required.
export JAVA_HOME=/usr/local/jdk

# Extra Java CLASSPATH elements.  Optional.
export HBASE_CLASSPATH=/usr/local/hbase

# The maximum amount of heap to use, in MB. Default is 1000.
#export HBASE_HEAPSIZE=1000

# Extra Java runtime options.
# Below are what we set by default.  May only work with SUN JVM.
# For more on why as well as other possible settings,
# see http://wiki.apache.org/hadoop/PerformanceTuning
export HBASE_OPTS="-server -XX:PermSize=64m -Xloggc:/data1/hbase/logs/hbase-gc.log"

# Uncomment below to enable java garbage collection logging in the .out file.
export HBASE_OPTS="$HBASE_OPTS -verbose:gc -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+PrintGCDetails"

# Uncomment below if you intend to use the EXPERIMENTAL off heap cache.
# export HBASE_OPTS="$HBASE_OPTS -XX:MaxDirectMemorySize="
# Set hbase.offheapcache.percentage in hbase-site.xml to a nonzero value.


# Uncomment and adjust to enable JMX exporting
# See jmxremote.password and jmxremote.access in $JRE_HOME/lib/management to configure remote password access.
# More details at: http://java.sun.com/javase/6/docs/technotes/guides/management/agent.html
#
#export HBASE_JMX_BASE="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
#HBASE_JMX_OPTS="$HBASE_JMX_OPTS -Dcom.sun.management.jmxremote.password.file=/usr/local/common/hbase/conf/jmxremote.passwd"
#HBASE_JMX_OPTS="$HBASE_JMX_OPTS -Dcom.sun.management.jmxremote.access.file=/usr/local/common/hbase/conf/jmxremote.access"
export SERVER_GC_OPTS="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:/data1/hbase/logs/server-gc.log.$(date +%Y%m%d%H%M%S) -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=512M"
export HBASE_MASTER_OPTS="-Xmx16g -Xmn1g -XX:SurvivorRatio=8"
export HBASE_REGIONSERVER_OPTS="-Xmx48g -Xms48g -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:-ResizePLAB -XX:+ParallelRefProcEnabled -XX:ParallelGCThreads=8 -XX:ConcGCThreads=2 -XX:InitiatingHeapOccupancyPercent=40 -XX:MaxTenuringThreshold=10"
#export HBASE_REGIONSERVER_OPTS="-Xms32g -Xmx32g -Xmn12288m -XX:SurvivorRatio=6 -XX:+UseConcMarkSweepGC -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=80 -XX:+ExplicitGCInvokesConcurrent -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=10 -XX:+UseFastAccessorMethods"
#export HBASE_THRIFT_OPTS="$HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10103"
#export HBASE_ZOOKEEPER_OPTS="$HBASE_JMX_BASE -Dcom.sun.management.jmxremote.port=10104"

# File naming hosts on which HRegionServers will run.  $HBASE_HOME/conf/regionservers by default.
# export HBASE_REGIONSERVERS=${HBASE_HOME}/conf/regionservers

# Extra ssh options.  Empty by default.
export HBASE_SSH_OPTS="-p 26387 -o ConnectTimeout=1 -o StrictHostKeyChecking=no -o SendEnv=HBASE_CONF_DIR"

# Where log files are stored.  $HBASE_HOME/logs by default.
export HBASE_LOG_DIR=/data1/hbase/logs

# A string representing this instance of hbase. $USER by default.
export HBASE_IDENT_STRING=hadoop

# The scheduling priority for daemon processes.  See 'man nice'.
# export HBASE_NICENESS=10

# The directory where pid files are stored. /tmp by default.
export HBASE_PID_DIR=/usr/local/hbase/pids

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export HBASE_SLAVE_SLEEP=0.1

# Tell HBase whether it should manage it's own instance of Zookeeper or not.
export HBASE_MANAGES_ZK=false

export HADOOP_HOME=/usr/local/hadoop
export HBASE_LIBRARY_PATH=/usr/local/hadoop/lib/native/Linux-amd64-64
