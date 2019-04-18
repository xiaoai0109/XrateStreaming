package example

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.flume.{ FlumeUtils, SparkFlumeEvent }
import org.apache.spark.streaming.{ Seconds, Minutes, StreamingContext }
import org.apache.spark.sql.{ SQLContext, SparkSession, DataFrame }
import org.apache.spark.sql.functions.{ col, udf }
import java.util.Arrays;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.{ Calendar, Date }
import java.text.SimpleDateFormat
import java.time.LocalDate

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

object Hello extends Greeting with App {
  println(greeting)

  //  BasicConfigurator.configure()
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.WARN)

  var sentimentModel: Double = 0.0
  var latestModel: String = ""
  getModelData()

  var i = 1

  val sparkSession = SparkSession.builder.master("local[3]")
    .appName("Xrate Streaming").getOrCreate()
  val sc = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext
  val ssc = new StreamingContext(sc, Minutes(1))
  val flumeStream = FlumeUtils.createStream(ssc, "127.0.0.1", 9990)
  val win = flumeStream.window(Minutes(3), Minutes(1))

  import sparkSession.implicits._

  win.foreachRDD { rdd =>
    println("\ncount: " + rdd.count()) // executed at the driver
    if (latestModel != LocalDate.now.toString() &&
      Calendar.getInstance().get(Calendar.HOUR_OF_DAY) == 10) {
      println("here")
      getModelData()
    }
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

  private def writeToHBase(xrate: Double, df: DataFrame) {
    //    val hConf = new HBaseConfiguration()
    val hConf = HBaseConfiguration.create()
    val hTable = new HTable(hConf, "sk:test1")
    val thePut = new Put(i.toString.getBytes())
    thePut.addColumn("i".getBytes(), "a".getBytes(), xrate.toString.getBytes())
    //    thePut.addColumn("i".getBytes(), "xrate".getBytes(), df.get(0).getBytes())
    hTable.put(thePut)
    i += 1

  }

  private def getModelData() {
    //    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    //    val key = dateFormatter.format(new Date())
    val key = LocalDate.now().toString
    val family = "sentiment"
    val column = "model"
    println(key)
    try {
      val hConf = HBaseConfiguration.create()
      val hTable = new HTable(hConf, "model_data")
      val theGet = new Get(key.getBytes())
      val result = hTable.get(theGet)
      val value = Bytes.toString(result.getValue(family.getBytes(), column.getBytes()))
      println("key:" + value)
      sentimentModel = value.toDouble
      println("model:" + sentimentModel)

      // TODO: Read Logistic Regression Model from Parquet

      latestModel = key
    } catch {
      case e: Throwable =>
        println(e.getMessage)
        println("Cannot get sentiment model of " + key)
    }
  }
}

trait Greeting {
  lazy val greeting: String = "Start to stream and predict the exchange rate"
}
