package example

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.{ FlumeUtils, SparkFlumeEvent }
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

object Hello extends Greeting with App {
  println(greeting)
  var i = 1

  val conf = new SparkConf().setAppName("Xrate Streaming").setMaster("local[2]")
  val sc = new StreamingContext(conf, Seconds(10))
  val flumeStream = FlumeUtils.createStream(sc, "127.0.0.1", 9990)
  flumeStream.foreachRDD(rdd => {
    println(rdd.count())
    rdd.foreach { record =>
      val data = new String(record.event.getBody().array())
      val xrate = getXrate(data)
      if (xrate != 0) predict(xrate)
      writeToHBase(xrate)
    }
  })

  sc.start()
  sc.awaitTermination()

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
