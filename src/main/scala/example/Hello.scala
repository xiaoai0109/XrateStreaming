package example

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.flume.{ FlumeUtils, SparkFlumeEvent }
import org.apache.spark.streaming.{ Seconds, Minutes, StreamingContext }
import org.apache.spark.sql.{ SQLContext, SparkSession, DataFrame, Row }
//import org.apache.spark.sql.functions.{ col, udf }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ StructType, StructField, StringType, FloatType, DoubleType, TimestampType }
import org.apache.spark.sql.expressions.Window
import java.util.Arrays;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.{ Calendar, Date }
import java.text.SimpleDateFormat
import java.time.LocalDate
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.{ LogisticRegression, LogisticRegressionModel }

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

object Hello extends Greeting with App {
  println(greeting)

  BasicConfigurator.configure()
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.WARN)

  var sentimentModel: Double = 0.0
  var latestModel: String = ""
  //  getModelData()

  var i = 1

  val sparkSession = SparkSession.builder.master("local[3]")
    .appName("Xrate Streaming").getOrCreate()
  val sc = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext
  //  val ssc = new StreamingContext(sc, Minutes(1))
  val ssc = new StreamingContext(sc, Seconds(20))
  val flumeStream = FlumeUtils.createStream(ssc, "127.0.0.1", 9990)
  //  val win = flumeStream.window(Minutes(3), Minutes(1))
  //    val win = flumeStream.window(Seconds(60), Seconds(20))

  val schema = StructType(
    StructField("datetime", StringType, true) ::
      StructField("xrate", DoubleType, true) :: Nil)
  var old1 = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
  var one = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
  var old2 = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
  var two = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
  var three = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
  var oldAddr = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
  var addResults = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)
//  var last = sqlContext.createDataFrame(sc.emptyRDD[Row], schema)

  import sparkSession.implicits._

  flumeStream.foreachRDD { rdd =>
    println("\ncount: " + rdd.count()) // executed at the driver
    if (latestModel != LocalDate.now.toString() &&
      Calendar.getInstance().get(Calendar.HOUR_OF_DAY) == 10) {
      getModelData()
    }
    rdd.foreach { x =>
      val data = new String(x.event.getBody().array())
      println(data)
      old1 = one
      one = sqlContext.read.schema(schema).json(Seq(data).toDS)

      old2 = two
      two = old1
      three = old2
      val unioned = one.union(two).union(three)
      unioned.show()
      predict(unioned)

    }
    /*
    rdd.foreach { record =>
      val data = new String(record.event.getBody().array())
      println(data)
      //    val data = """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}"""
      //    println(df.select(col("address").getItem("state").as("state")).show())
      val df = sqlContext.read.json(Seq(data).toDS)
      //      println(df.show())
      val xrateDf = df.select(col("Realtime Currency Exchange Rate").getItem("6. Last Refreshed").as("datetime"), col("Realtime Currency Exchange Rate").getItem("5. Exchange Rate").as("xrate"))

      xrateDf.printSchema()
      xrateDf.show()
    }

    */
  }

  ssc.start()
  ssc.awaitTermination()

  private def predict(df: DataFrame) {
    val w = Window.partitionBy().orderBy("datetime")
    val sentmentindex = 0.65

    val addt = df
      .withColumn("t-1", lag("xrate", 1).over(w))
      .withColumn("t-2", lag("xrate", 2).over(w)).na.drop()
      .withColumn("sentiment", lit(sentmentindex))
//    addt.show()

    if (!addt.take(1).isEmpty) {
      val assembler = new VectorAssembler()
        .setInputCols(Array("xrate", "t-1", "t-2", "sentiment"))
        .setOutputCol("features")

      val vectorised = assembler.transform(addt)
//      vectorised.show()

      val sameModel = LogisticRegressionModel.load("myModelPath")
      val results = sameModel.transform(vectorised)
//      results.show()

      if (!addResults.take(1).isEmpty) {
        oldAddr = addResults
        addResults = results.withColumn("changes", (col("xrate") - col("t-1")) / col("t-1"))
          .withColumn("signal", when(col("prediction") === 1, 1).otherwise(-1))

        val last = oldAddr.union(addResults)
          .withColumn("returns", lag("signal", 1, 0).over(w) * col("changes") + 1)
          .withColumn("cumulativeReturns", exp(sum(log(col("returns"))).over(w)))
          
        last.show()
        writeToHBase(last.orderBy(desc("datetime")).limit(1))
      } else {
        addResults = results.withColumn("changes", (col("xrate") - col("t-1")) / col("t-1"))
          .withColumn("signal", when(col("prediction") === 1, 1).otherwise(-1))
//        last = addResults
//          .withColumn("returns", lit(1))
//          .withColumn("cumulativeReturns", exp(sum(log(col("returns"))).over(w)))
//          .orderBy(desc("datetime")).limit(1)
      }
    }
  }

  private def writeToHBase(df: DataFrame) {
    /*
    val hConf = HBaseConfiguration.create()
    val hTable = new HTable(hConf, "sk:test1")
    val thePut = new Put(i.toString.getBytes())
    thePut.addColumn("i".getBytes(), "a".getBytes(), xrate.toString.getBytes())
    //    thePut.addColumn("i".getBytes(), "xrate".getBytes(), df.get(0).getBytes())
    hTable.put(thePut)
    i += 1
*/

    df.foreachPartition { iter =>
      val hConf = HBaseConfiguration.create()
      val hTable = new HTable(hConf, "xrate_prediction")
      iter.foreach { item =>
        val rowKey = item.get(0).toString
        val thePut = new Put(rowKey.getBytes())
        thePut.addColumn("sum".getBytes(), "xrate".getBytes(), item.get(1).toString.getBytes())
        thePut.addColumn("sum".getBytes(), "signal".getBytes(), item.get(10).toString.getBytes())
        thePut.addColumn("sum".getBytes(), "returns".getBytes(), item.get(11).toString.getBytes())
        thePut.addColumn("sum".getBytes(), "cumulativeReturns".getBytes(), item.get(12).toString.getBytes())
        hTable.put(thePut)
        println("Write to HBase successfully")
      }
    }

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
