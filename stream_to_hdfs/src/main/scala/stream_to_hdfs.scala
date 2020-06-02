
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
    // CODE SAVE MESSAGE TO CSV 

    val dfCsv = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "general")
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
    val querycsv = valuesplitCsv.writeStream.format("csv")
      .option("header", "true")
      .trigger(Trigger.Once)
      .option("checkpointLocation","checkpoint")
      .start("message_save/message-"+date+".csv")

      querycsv.awaitTermination()
      print("Job finished")
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
      .getOrCreate()

    saveCsv(spark)
  }
}
