set -x
mkdir /tmp/spark-events
source ./env.sh
/Users/ccompain/Documents/code/apache/spark-3.0.1-bin-hadoop2.7/sbin/start-all.sh
/Users/ccompain/Documents/code/apache/spark-3.0.1-bin-hadoop2.7/sbin/start-history-server.sh
jps

