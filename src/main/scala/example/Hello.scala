package example

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.flume.{ FlumeUtils, SparkFlumeEvent }
import org.apache.spark.streaming.{ Seconds, Minutes, StreamingContext }
import org.apache.spark.sql.{ SQLContext, SparkSession }
import org.apache.spark.sql.functions.{col, udf}
import java.util.Arrays;
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

object Hello extends Greeting with App {
  println(greeting)
  var i = 1
  
  val sparkSession = SparkSession.builder.master("local[3]")
    .appName("Xrate Streaming").getOrCreate()
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.WARN)
  val sc = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext
  val ssc = new StreamingContext(sc, Minutes(1)) 
  val flumeStream = FlumeUtils.createStream(ssc, "127.0.0.1", 9990)
  val win = flumeStream.window(Minutes(3), Minutes(1))

  import sparkSession.implicits._

  win.foreachRDD { rdd =>
  println("\ncount: " + rdd.count())  // executed at the driver
  rdd.foreach { record =>
    val data = new String(record.event.getBody().array()) 
    println(data)
//    val data = """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}"""
//    println(df.select(col("address").getItem("state").as("state")).show())
    val df = sqlContext.read.json(Seq(data).toDS)
    println(df.select(col("RealtimeCurrencyExchangeRate").getItem("5.ExchangeRate").as("xrate")).show())
//    df.printSchema()
  }
}

  ssc.start()
  ssc.awaitTermination()

  private def predict(xrate: Double) {
    println(xrate)
  }

  private def getXrate(data: String): Double = {
    try {
      val array = data.split(",")
      val xrate = array(1).toDouble
      xrate
    } catch {
      case _: Throwable => 0
    }
  }

  private def writeToHBase(xrate: Double) {
    val hConf = new HBaseConfiguration()
    val hTable = new HTable(hConf, "sk:test1")
    val thePut = new Put(i.toString.getBytes())
    thePut.addColumn("i".getBytes(), "a".getBytes(), xrate.toString.getBytes())
    hTable.put(thePut)
    i += 1
  }
}

trait Greeting {
  lazy val greeting: String = "Start to stream and predict the exchange rate"
}
