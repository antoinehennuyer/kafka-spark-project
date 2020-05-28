
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object stream_to_hdfs {

  def main(args: Array[String]) {
    /* println( "Trying to write to HDFS..." )
    val conf = new Configuration()
    //conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020")
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName);
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName);

    val fs= FileSystem.get(conf)
    val output = fs.create(new Path("/tmp/mySample.txt"))
    val writer = new PrintWriter(output)
    try {
      writer.write("this is a test")
      writer.write("\n")
    }
    finally {
      writer.close()
      println("Closed!")
    }
    println("Done!")
  }*/



    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
      .getOrCreate()

//    val mySchema = StructType(Array(
//      StructField("id", IntegerType),
//      StructField("location", StringType),
//    ))

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
    val valuesplit = valuedf.selectExpr("value.ID", "value.location","value.time","value.violation_code", "value.state", "value.vehiculeMake", "value.batteryPercent", "value.temperatureDrone", "value.mType")
    valuesplit.write.format("csv").option("header", "true").save("test.csv")
    //valuesplit.show(20)

    //personJsonDf.show(20)

    //df.show(10)
  }


}
