
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration.Duration

object stream_to_hdfs {

  def saveCsv(spark: SparkSession) = {
    import spark.implicits._
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.format(Calendar.getInstance().getTime())
    // CODE SAVE DRONE MSG
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "general")
      .option("startingOffsets","earliest")
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
    //print(valuedf)
    val valuesplit = valuedf.selectExpr("value.ID", "value.location","value.time","value.violation_code",
      "value.state", "value.vehiculeMake", "value.batteryPercent", "value.temperatureDrone", "value.mType")
    val query = valuesplit.writeStream.format("csv").option("header", "true").trigger(Trigger.Once).option("checkpointLocation","checkpoint").start("drone_save/drone-"+date+".csv")
    query.awaitTermination()

// CODE SAVE CSV NYPD

  val dfCsv = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sendCSV")
      .option("startingOffsets","earliest")
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
    val querycsv = valuesplitCsv.writeStream.format("csv").option("header", "true").trigger(Trigger.Once).option("checkpointLocation","checkpoint").start("drone_save/CSVFile-"+date+".csv")
      querycsv.awaitTermination()
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
      .getOrCreate()

    import spark.implicits._


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
