import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object analyser{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Analyser").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val df = spark.read.csv("result.csv").toDF()
    df.show()
    // RUN ANALYSIS HERE
    spark.close()
  }

}