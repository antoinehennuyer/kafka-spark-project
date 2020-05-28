
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object stream_to_hdfs {

  def main(args: Array[String]) {



    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
      .getOrCreate()



    import spark.implicits._

    //CODE FOR CONSUMER CSV
    val dfCsv = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "sendCsv")
      .load()



    val JsonDfCsv = dfCsv.selectExpr("CAST(value AS STRING)")
    val structCsv = new StructType()
      .add("ID", DataTypes.StringType)
      .add("location", DataTypes.StringType)
      .add("time",DataTypes.StringType)
      .add("violation_code",DataTypes.StringType)
      .add("state", DataTypes.StringType)

    val valuedfCsv = JsonDfCsv.select(from_json($"value", structCsv).as("value"))
    val valuesplitCsv = valuedfCsv.selectExpr("value.ID", "value.location","value.time","value.violation_code", "value.state")
    valuesplitCsv.write.format("csv").option("header", "true").save("CSVFile.csv")
  }

}
