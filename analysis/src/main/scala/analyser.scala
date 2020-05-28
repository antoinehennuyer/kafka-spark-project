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
    val df = spark.read.option("header", "true").csv("Parking_Violations_Issued_-_Fiscal_Year_2017.csv").toDF()
    // df.show()
    // RUN ANALYSIS HERE

    // val violation = df.groupBy("violation_code").count()

    val state = df.groupBy("Registration State").count().orderBy(desc("count"))
    val carType = df.groupBy("Vehicle Make").count().orderBy(desc("count"))
    val infraction = df.groupBy("Violation Code").count().orderBy(desc("count"))
    state.show(5)
    carType.show(5)
    infraction.show(5)
    // violation.where("count > 3").show()
    // val test = df.filter($"violation_code" === "alert").groupBy("violation_code").count().show()
    // val violation2 = df.groupBy("violation_code").where("violation_code == test").count().show()
    spark.close()
  }

}
