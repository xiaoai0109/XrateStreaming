import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val streamingFlume = "org.apache.spark" %% "spark-streaming-flume" % "2.2.0"
  lazy val sparkStreaming = "org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided"
  lazy val sparkCore =  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
  lazy val hbaseCommon = "org.apache.hbase" % "hbase-common" % "1.2.0" % "provided"
  lazy val hbaseClient = "org.apache.hbase" % "hbase-client" % "1.2.0" % "provided"
  lazy val sparkSQL = "org.apache.spark" %% "spark-sql" % "2.2.0"
}
