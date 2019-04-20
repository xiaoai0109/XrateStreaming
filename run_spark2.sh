#sbt clean package
#hdfs dfs -put myModelPath/ /user/cloudera/

#spark2-submit --master local[2] --class example.Hello --packages org.apache.hbase:hbase-common:1.2.0,org.apache.spark:spark-streaming-flume_2.11:2.2.0,org.apache.hbase:hbase-client:1.2.0,org.apache.spark:spark-sql_2.12:2.2.0,org.apache.spark:spark-mllib_2.12:2.2.0 --name XrateStreaming hbase_xratestreaming_2.11-0.1.0-SNAPSHOT.jar 

#spark2-submit --master local[2] --class example.Hello --packages org.apache.hbase:hbase-common:1.2.0,org.apache.spark:spark-streaming-flume_2.11:2.2.0,org.apache.hbase:hbase-client:1.2.0,org.apache.spark:spark-sql_2.12:2.2.0,org.apache.spark:spark-mllib_2.12:2.2.0,org.mongodb.spark:mongo-spark-connector_2.11:2.4.0 --name XrateStreaming 20s_mongoDB_xratestreaming.jar  

spark2-submit --master local[2] --class example.Hello --packages org.apache.hbase:hbase-common:1.2.0,org.apache.spark:spark-streaming-flume_2.11:2.2.0,org.apache.hbase:hbase-client:1.2.0,org.apache.spark:spark-sql_2.12:2.2.0,org.apache.spark:spark-mllib_2.12:2.2.0,org.mongodb.spark:mongo-spark-connector_2.11:2.4.0 --name XrateStreaming 1min_mongoDB_xratestreaming.jar  
