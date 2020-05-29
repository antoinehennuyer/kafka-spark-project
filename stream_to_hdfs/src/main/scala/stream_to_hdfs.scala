
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.concurrent.duration.Duration

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
    val actorSystem = ActorSystem()
    val scheduler = actorSystem.scheduler
    val task = new Runnable { def run() { saveCsv(spark) } }
    implicit val executor = actorSystem.dispatcher

    scheduler.schedule(
      initialDelay = Duration(0, TimeUnit.SECONDS),
      interval = Duration(5, TimeUnit.SECONDS),
      runnable = task)
  }
}