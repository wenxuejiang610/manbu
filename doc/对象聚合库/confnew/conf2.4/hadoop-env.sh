# Set Hadoop-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.
export JAVA_HOME="/usr/local/jdk"

# The jsvc implementation to use. Jsvc is required to run secure datanodes.
#export JSVC_HOME=${JSVC_HOME}

export HADOOP_CONF_DIR="/usr/local/common/hadoop/conf"

# Extra Java CLASSPATH elements.  Automatically insert capacity-scheduler.
#for f in $HADOOP_HOME/contrib/capacity-scheduler/*.jar; do
#  if [ "$HADOOP_CLASSPATH" ]; then
#    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$f
#  else
#    export HADOOP_CLASSPATH=$f
#  fi
#done

# The maximum amount of heap to use, in MB. Default is 1000.
#export HADOOP_HEAPSIZE=
#export HADOOP_NAMENODE_INIT_HEAPSIZE=""
export HADOOP_NAMENODE_OPTS="-Xmx16g"
export HADOOP_SECONDARYNAMENODE_OPTS="-Xmx16g"
export HADOOP_DATANODE_OPTS="-Xmx2g"
#export HADOOP_JOBTRACKER_OPTS="-Xmx16g"
#export HADOOP_TASKTRACKER_OPTS="-Xmx2g"

# Extra Java runtime options.  Empty by default.
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

# Command specific options appended to HADOOP_OPTS when specified
export HADOOP_NAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_NAMENODE_OPTS $HADOOP_OPTS"
export HADOOP_SECONDARYNAMENODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_SECONDARYNAMENODE_OPTS $HADOOP_OPTS"
export HADOOP_DATANODE_OPTS="-Dcom.sun.management.jmxremote $HADOOP_DATANODE_OPTS $HADOOP_OPTS"
export HADOOP_BALANCER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_BALANCER_OPTS $HADOOP_OPTS"
export HADOOP_JOBTRACKER_OPTS="-Dcom.sun.management.jmxremote $HADOOP_JOBTRACKER_OPTS $HADOOP_OPTS"
export HADOOP_TASKTRACKER_OPTS="$HADOOP_TASKTRACKER_OPTS"

export HADOOP_NFS3_OPTS="$HADOOP_NFS3_OPTS"
export HADOOP_PORTMAP_OPTS="-Xmx512m $HADOOP_PORTMAP_OPTS"

# The following applies to multiple commands (fs, dfs, fsck, distcp, ssh etc)
export HADOOP_CLIENT_OPTS="-Xmx512m $HADOOP_CLIENT_OPTS"
export HADOOP_SSH_OPTS="-p 26387 -o ConnectTimeout=1 -o SendEnv=HADOOP_CONF_DIR -o StrictHostKeyChecking=no"

# On secure datanodes, user to run the datanode as after dropping privileges
#export HADOOP_SECURE_DN_USER="hadoop"

# Where log files are stored.  $HADOOP_HOME/logs by default.
export HADOOP_LOG_DIR="/data1/hadoop/logs"

# Where log files are stored in the secure data environment.
export HADOOP_SECURE_DN_LOG_DIR=${HADOOP_LOG_DIR}/${HADOOP_HDFS_USER}

# The directory where pid files are stored. /tmp by default.
# NOTE: this should be set to a directory that can only be written to by 
#       the user that will run the hadoop daemons.  Otherwise there is the
#       potential for a symlink attack.
export HADOOP_PID_DIR=/usr/local/hadoop/pids
#export HADOOP_SECURE_DN_USER="hadoop"
export HADOOP_SECURE_DN_PID_DIR=${HADOOP_PID_DIR}

# A string representing this instance of hadoop. $USER by default.
export HADOOP_IDENT_STRING=$USER
export HADOOP_NAMENODE_USER=hadoop
export HADOOP_DATANODE_USER=hadoop
export HADOOP_JOBTRACKER_USER=hadoop
export HADOOP_TASKTRACKER_USER=hadoop
