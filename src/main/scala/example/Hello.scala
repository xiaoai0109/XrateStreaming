package example

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.flume.{ FlumeUtils, SparkFlumeEvent }
import org.apache.spark.streaming.{ Seconds, Minutes, StreamingContext }
import org.apache.spark.sql.{ SQLContext, SparkSession, DataFrame, Row }
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
import java.sql.Timestamp
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.{ LogisticRegression, LogisticRegressionModel }

import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName }
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import com.mongodb.spark._

object Hello extends Greeting with App {
  println(greeting)

  BasicConfigurator.configure()
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  var sentimentIndex: Double = _
  var lrModel: LogisticRegressionModel = _
  var latestModel: String = _

  val sparkSession = SparkSession.builder.master("local[3]")
    .config("spark.mongodb.input.uri", "mongodb://localhost/Sentiments.sentiments")
    .config("spark.mongodb.output.uri", "mongodb://localhost/BloombergNews.news")
    .appName("Xrate Streaming").getOrCreate()
  val sc = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext
  val ssc = new StreamingContext(sc, Minutes(1))
//  val ssc = new StreamingContext(sc, Seconds(20))
  val flumeStream = FlumeUtils.createStream(ssc, "127.0.0.1", 9990)

  getModelData()
  
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

  import sparkSession.implicits._

  flumeStream.foreachRDD { rdd =>
    println("count: " + rdd.count()) // executed at the driver
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
      //unioned.show()
      predict(unioned)

    }
  }

  ssc.start()
  ssc.awaitTermination()

  private def predict(df: DataFrame) {
    val w = Window.partitionBy().orderBy("datetime")

    val addt = df
      .withColumn("t-1", lag("xrate", 1).over(w))
      .withColumn("t-2", lag("xrate", 2).over(w)).na.drop()
      .withColumn("sentiment", lit(sentimentIndex))

    if (!addt.take(1).isEmpty) {
      val assembler = new VectorAssembler()
        .setInputCols(Array("xrate", "t-1", "t-2", "sentiment"))
        .setOutputCol("features")

      val vectorised = assembler.transform(addt)
      val results = lrModel.transform(vectorised)

      if (!addResults.take(1).isEmpty) {
        oldAddr = addResults
        addResults = results.withColumn("changes", (col("xrate") - col("t-1")) / col("t-1"))
          .withColumn("signal", when(col("prediction") === 1, 1).otherwise(-1))
          .withColumn("returns", lit(oldAddr.first().getInt(10)) * col("changes") + 1)
          .withColumn("cumulativeReturns", lit(oldAddr.first().getDouble(12)) * col("returns"))
      } else {
        addResults = results.withColumn("changes", (col("xrate") - col("t-1")) / col("t-1"))
          .withColumn("signal", when(col("prediction") === 1, 1).otherwise(-1))
          .withColumn("returns", lit(1))
          .withColumn("cumulativeReturns", exp(sum(log(col("returns"))).over(w)))
      }
      val stored = addResults.select("datetime", "xrate", "changes", "signal", "returns", "cumulativeReturns")
      stored.show()
      writeToMongoDB(stored)
    }
  }
  
  private def writeToMongoDB(df: DataFrame) {
    df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","Prediction").option("collection", "prediction").save()
  }
  
  private def getModelData() {
      val df = MongoSpark.load(sparkSession) 
      sentimentIndex = df.orderBy(desc("date")).first.getDouble(2)
      lrModel = LogisticRegressionModel.load("myModelPath")
      latestModel = LocalDate.now().toString
  }
}

trait Greeting {
  lazy val greeting: String = "Start to stream the exchange rate and predict"
}
