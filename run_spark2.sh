#sbt clean package

spark2-submit --master local[2] --class example.Hello --packages org.apache.hbase:hbase-common:1.2.0,org.apache.spark:spark-streaming-flume_2.11:2.2.0,org.apache.hbase:hbase-client:1.2.0 --name XrateStreaming target/scala-2.11/xratestreaming_2.11-0.1.0-SNAPSHOT.jar 
