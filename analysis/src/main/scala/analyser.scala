import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object analyser{

  def main(args: Array[String]): Unit = {

    val file = args(0)

    val conf = new SparkConf().setAppName("Analyser").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val df = spark.read.option("header", "true").csv(file).toDF()
    // RUN ANALYSIS HERE

    // Quelle est la région qui a le plus de contraventions ?
    val state = df.groupBy("state").count().orderBy(desc("count"))

    // Quel type de voiture a plus de contraventions ?
    val carType = df.groupBy("vehiculeMake").count().orderBy(desc("count"))

    // Quelles sont les contraventions les plus fréquentes ?
    val infraction = df.groupBy("violation_code").count().orderBy(desc("count"))

    // Quels sont les mois où il y a eut le plus de contraventions ?
    val splitTime = df.withColumn("temp", split(col("time"), "/")).select((0 until 3).map(i => col("temp").getItem(i).as(s"col$i")): _*)
    val mounth = splitTime.groupBy("col0").count().orderBy(desc("count")).withColumnRenamed("col0", "mounth")

    state.show(5)
    carType.show(5)
    infraction.show(5)
    mounth.show()
    spark.close()
  }

}
