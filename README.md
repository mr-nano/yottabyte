# Yottabyte

REPAIR_INPUT_FILE_LOCATION=test/resources/repaircurrencyconversion/REPAIR_IN_DIFFERENT_CURRENCIES
HDFS_VEHICLE_INPUT_FILE_LOCATION=hdfs://yottabyte02.thoughtworks.com:8020/user/vaibhavk/vehicleCount/input/VEHICLE_INFO

#Maven command to package
mvn clean package -Duser=YOUR_USERNAME

#Copying the artifact to edge node.
scp target/hadoop-1.0-SNAPSHOT-jar-with-dependencies.jar YOUR_USERNAME@10.10.5.96:~

#HDFS command to copy input file
hadoop dfs -cp hdfs://yottabyte02.thoughtworks.com:8020/user/vaibhavk/vehicleCount/input/VEHICLE_INFO vehicleCount/input/VEHICLE_INFO

#Hadoop command to run the job on EdgeNode
hadoop jar hadoop-1.0-SNAPSHOT-jar-with-dependencies.jar MAIN_CLASS_NAME

#Spark shell 
HDFS_USER_NAME=YOUR_USERNAME HADOOP_CONF_DIR=/etc/hadoop/conf spark-shell --master yarn-client --conf spark.ui.port=YOUR_EMP_ID

#Spark job submit
spark-submit --class class-name APPLICATION_JAR_PATH APPLICATION_PARAMETERS

EDGE_NODE=10.10.5.96
NAMENODE=10.10.5.97

RESOURCEMANAGER=10.10.5.95
JOB_HISTORY_SERVER=10.10.5.95



