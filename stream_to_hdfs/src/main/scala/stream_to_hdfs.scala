
 import org.apache.spark
 import org.apache.spark.SparkConf
 import org.apache.spark.{SparkConf, SparkContext, sql}
 import org.apache.spark.rdd.RDD
 import org.apache.spark.sql.types.{DataTypes, DoubleType, IntegerType, StringType, StructField, StructType}
 import org.apache.spark.sql.{DataFrame, SparkSession}
 import org.apache.spark.sql.functions._

 import scala.concurrent.duration.Duration

 /*import akka.actor.Actor
 import akka.actor.ActorSystem
 import akka.actor.Props*/


 object stream_to_hdfs {

  def saveCsv(spark: SparkSession) = {
    import spark.implicits._

    val dfCsv = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sendCSV")
      .load()

    val JsonDfCsv = dfCsv.selectExpr("CAST(value AS STRING)")
    val structCsv = new StructType()
      .add("ID", DataTypes.StringType)
      .add("location", DataTypes.StringType)
      .add("time",DataTypes.StringType)
      .add("violation_code",DataTypes.StringType)
      .add("state", DataTypes.StringType)
      .add("vehiculeMake", DataTypes.StringType)
      .add("batteryPercent", DataTypes.StringType)
      .add("temperatureDrone", DataTypes.StringType)
      .add("mType", DataTypes.StringType)

    val valuedfCsv = JsonDfCsv.select(from_json($"value", structCsv).as("value"))
    val valuesplitCsv = valuedfCsv.selectExpr("value.ID", "value.location","value.time","value.violation_code",
      "value.state", "value.vehiculeMake", "value.batteryPercent", "value.temperatureDrone", "value.mType")
    valuesplitCsv.write.format("csv").option("header", "true").save("CSVFile.csv")
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "general")
      .load()

    val JsonDf = df.selectExpr("CAST(value AS STRING)")
    val struct = new StructType()
      .add("ID", DataTypes.StringType)
      .add("location", DataTypes.StringType)
      .add("time",DataTypes.StringType)
      .add("violation_code",DataTypes.StringType)
      .add("state", DataTypes.StringType)
      .add("vehiculeMake", DataTypes.StringType)
      .add("batteryPercent", DataTypes.StringType)
      .add("temperatureDrone", DataTypes.StringType)
      .add("mType", DataTypes.StringType)

    val valuedf = JsonDf.select(from_json($"value", struct).as("value"))
    val valuesplit = valuedf.selectExpr("value.ID", "value.location","value.time","value.violation_code",
      "value.state", "value.vehiculeMake", "value.batteryPercent", "value.temperatureDrone", "value.mType")
    valuesplit.write.format("csv").option("header", "true").save("test.csv")

    //CODE FOR CONSUMER CSV
    /*val actorSystem = ActorSystem()
    val scheduler = actorSystem.scheduler
    val task = new Runnable { def run() { saveCsv(spark) } }
    implicit val executor = actorSystem.dispatcher*/
    saveCsv(spark)

    /*scheduler.schedule(
      initialDelay = Duration(0, TimeUnit.SECONDS),
      interval = Duration(10, TimeUnit.SECONDS),
      runnable = task)*/
    saveCsv(spark)
    /*val dfCsv = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sendCSV")
      .load()

    val JsonDfCsv = dfCsv.selectExpr("CAST(value AS STRING)")
    val structCsv = new StructType()
      .add("ID", DataTypes.StringType)
      .add("location", DataTypes.StringType)
      .add("time",DataTypes.StringType)
      .add("violation_code",DataTypes.StringType)
      .add("state", DataTypes.StringType)
      .add("vehiculeMake", DataTypes.StringType)
      .add("batteryPercent", DataTypes.StringType)
      .add("temperatureDrone", DataTypes.StringType)
      .add("mType", DataTypes.StringType)

    val valuedfCsv = JsonDfCsv.select(from_json($"value", structCsv).as("value"))
    val valuesplitCsv = valuedfCsv.selectExpr("value.ID", "value.location","value.time","value.violation_code",
      "value.state", "value.vehiculeMake", "value.batteryPercent", "value.temperatureDrone", "value.mType")
    valuesplitCsv.write.format("csv").option("header", "true").save("CSVFile.csv")*/
  }


}
