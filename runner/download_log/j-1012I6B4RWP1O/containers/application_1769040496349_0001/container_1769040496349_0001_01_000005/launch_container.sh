#!/bin/bash

set -o pipefail -e
export PRELAUNCH_OUT="/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005/prelaunch.out"
exec >"${PRELAUNCH_OUT}"
export PRELAUNCH_ERR="/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005/prelaunch.err"
exec 2>"${PRELAUNCH_ERR}"
echo "Setting up env variables"
export JAVA_HOME=${JAVA_HOME:-"/usr/lib/jvm/jre-17"}
export HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME:-"/usr/lib/hadoop"}
export HADOOP_HDFS_HOME=${HADOOP_HDFS_HOME:-"/usr/lib/hadoop-hdfs"}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-"/etc/hadoop/conf"}
export HADOOP_YARN_HOME=${HADOOP_YARN_HOME:-"/usr/lib/hadoop-yarn"}
export PATH=${PATH:-"/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"}
export HADOOP_TOKEN_FILE_LOCATION="/mnt1/yarn/usercache/hadoop/appcache/application_1769040496349_0001/container_1769040496349_0001_01_000005/container_tokens"
export CONTAINER_ID="container_1769040496349_0001_01_000005"
export NM_PORT="8041"
export NM_HOST="ip-172-31-43-87.ec2.internal"
export NM_HTTP_PORT="8042"
export LOCAL_DIRS="/mnt/yarn/usercache/hadoop/appcache/application_1769040496349_0001,/mnt1/yarn/usercache/hadoop/appcache/application_1769040496349_0001"
export LOCAL_USER_DIRS="/mnt/yarn/usercache/hadoop/,/mnt1/yarn/usercache/hadoop/"
export LOG_DIRS="/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005"
export USER="hadoop"
export LOGNAME="hadoop"
export HOME="/home/"
export PWD="/mnt1/yarn/usercache/hadoop/appcache/application_1769040496349_0001/container_1769040496349_0001_01_000005"
export LOCALIZATION_COUNTERS="0,334824,0,2,6"
export JVM_PID="$$"
export NM_AUX_SERVICE_mapreduce_shuffle="AAA0+gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
export STDOUT_LOGFILE_ENV="/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005/stdout"
export SHELL="/bin/bash"
export HADOOP_ROOT_LOGGER="INFO,console"
export HADOOP_MAPRED_HOME="/usr/lib/hadoop-mapreduce"
export CLASSPATH="$PWD:$HADOOP_CONF_DIR:$HADOOP_COMMON_HOME/*:$HADOOP_COMMON_HOME/lib/*:$HADOOP_HDFS_HOME/*:$HADOOP_HDFS_HOME/lib/*:$HADOOP_MAPRED_HOME/*:$HADOOP_MAPRED_HOME/lib/*:$HADOOP_YARN_HOME/*:$HADOOP_YARN_HOME/lib/*:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/lib/*:/usr/share/aws/emr/ddb/lib/emr-ddb-hadoop.jar:/usr/share/aws/emr/goodies/lib/emr-hadoop-goodies.jar:/usr/share/aws/emr/kinesis/lib/emr-kinesis-hadoop.jar:/usr/share/aws/emr/cloudwatch-sink/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/aws-java-sdk-v2/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*:/usr/share/aws/emr/lib/*:/usr/share/aws/emr/ddb/lib/emr-ddb-hadoop.jar:/usr/share/aws/emr/goodies/lib/emr-hadoop-goodies.jar:/usr/share/aws/emr/kinesis/lib/emr-kinesis-hadoop.jar:/usr/share/aws/emr/cloudwatch-sink/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/aws-java-sdk-v2/*:job.jar/*:job.jar/classes/:job.jar/lib/*:$PWD/*"
export LD_LIBRARY_PATH="$PWD:/usr/lib/hadoop/lib/native:/usr/lib/hadoop-lzo/lib/native"
export STDERR_LOGFILE_ENV="/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005/stderr"
export HADOOP_CLIENT_OPTS=""
export MALLOC_ARENA_MAX="4"
echo "Setting up job resources"
ln -sf -- "/mnt/yarn/usercache/hadoop/appcache/application_1769040496349_0001/filecache/13/job.xml" "job.xml"
ln -sf -- "/mnt/yarn/usercache/hadoop/appcache/application_1769040496349_0001/filecache/11/job.jar" "job.jar"
echo "Copying debugging information"
# Creating copy of launch script
cp "launch_container.sh" "/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005/launch_container.sh"
chmod 640 "/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005/launch_container.sh"
# Determining directory contents
echo "ls -l:" 1>"/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005/directory.info"
ls -l 1>>"/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005/directory.info"
echo "find -L . -maxdepth 5 -ls:" 1>>"/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005/directory.info"
find -L . -maxdepth 5 -ls 1>>"/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005/directory.info"
echo "broken symlinks(find -L . -maxdepth 5 -type l -ls):" 1>>"/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005/directory.info"
find -L . -maxdepth 5 -type l -ls 1>>"/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005/directory.info"
echo "Launching container"
exec /bin/bash -c "$JAVA_HOME/bin/java -Djava.net.preferIPv4Stack=true -Dhadoop.metrics.log.level=WARN  -Xmx2458m --add-opens=java.base/java.lang=ALL-UNNAMED --add-exports=java.base/sun.net.dns=ALL-UNNAMED --add-exports=java.base/sun.net.util=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.regex=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/javax.script=ALL-UNNAMED -Dillegal-access=permit --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens=java.base/jdk.internal.util.random=ALL-UNNAMED --add-opens=java.base/jdk.internal.util=ALL-UNNAMED --add-opens=java.base/jdk.internal=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.sql/java.sql=ALL-UNNAMED --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED -Djava.io.tmpdir=$PWD/tmp -Dlog4j.configuration=container-log4j.properties -Dyarn.app.container.log.dir=/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005 -Dyarn.app.container.log.filesize=0 -Dhadoop.root.logger=INFO,CLA -Dhadoop.root.logfile=syslog org.apache.hadoop.mapred.YarnChild 172.31.43.87 42613 attempt_1769040496349_0001_m_000003_0 5 1>/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005/stdout 2>/var/log/hadoop-yarn/containers/application_1769040496349_0001/container_1769040496349_0001_01_000005/stderr "
