package example

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.flume.{ FlumeUtils, SparkFlumeEvent }
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.sql.{ SQLContext, SparkSession }
import org.apache.spark.sql.functions._
import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

object Hello extends Greeting with App {
  println(greeting)
  var i = 1
/*
  val sparkSession = SparkSession.builder.master("local")
    .appName("Xrate Streaming").getOrCreate()
  val sc = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext
  val ssc = new StreamingContext(sc, Seconds(10)) 
  val flumeStream = FlumeUtils.createStream(ssc, "127.0.0.1", 9990)
    
  import sparkSession.implicits._

  flumeStream.foreachRDD { rdd =>
  println(rdd.count())  // executed at the driver
  rdd.foreach { record =>
    val data = new String(record.event.getBody().array()) 
    println(data)
//      val data = """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}"""
//    val df = sqlContext.read.json(Seq(data).toDS)
//    println(df.show())
//    df.printSchema()
  }
}
*/
  

  val conf = new SparkConf().setAppName("Xrate Streaming").setMaster("local[2]")
  val ssc = new StreamingContext(conf, Seconds(10))

  val flumeStream = FlumeUtils.createStream(ssc, "127.0.0.1", 9990)
  flumeStream.foreachRDD(rdd => {
    println(rdd.count())
    rdd.foreach { record =>
      val data = new String(record.event.getBody().array())
      val xrate = getXrate(data)
      if (xrate != 0) predict(xrate)
      writeToHBase(xrate)
    }
  })

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
  lazy val greeting: String = "hello"
}
