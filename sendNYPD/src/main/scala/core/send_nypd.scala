package core

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import play.api.libs.json.{JsString, Json}
import utils.MessageUtils
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Random

object send_nypd {



  def CreateNypdMessages(): Any = {
    val (prod,spark) = initiateProducer()
    val nypdCsv = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(".")

//    val struct = new StructType()
//      .add("ID", DataTypes.StringType)
//      .add("location", DataTypes.StringType)
//      .add("time",DataTypes.StringType)
//      .add("violation_code",DataTypes.StringType)
//      .add("state", DataTypes.StringType)
//      .add("vehiculeMake", DataTypes.StringType)
//      .add("batteryPercent", DataTypes.StringType)
//      .add("temperatureDrone", DataTypes.StringType)
//      .add("mType", DataTypes.StringType)

    import spark.implicits._

    val valuedf = nypdCsv.select("Violation Location","Violation Time","Violation Code","Violation County")

    valuedf.foreach { line =>

      val loc = line.mkString(",").split(",")(0)
      val time = line.mkString(",").split(",")(1)
      val vioCode = line.mkString(",").split(",")(2)
      MessageGenerate("NA",loc,time,vioCode,prod)
    }

    prod.close()
  }

  def MessageGenerate(id: String, loc: String, time: String, vioCode: String, prod: KafkaProducer[String,String]): Any = {
    val msg = MessageUtils.Message(id, loc, time, vioCode, "", "", "","","NYPD")
    println(msg)
    sendMessage(msg, prod)
  }

  def sendMessage(msg : MessageUtils.Message, prod: KafkaProducer[String,String]): Any = {
    val JSON = Json.obj("ID"->JsString(msg.id), "location"->JsString(msg.location),
      "time"->JsString(msg.time), "violation_code"->JsString(msg.violationCode),
      "state"->JsString(""), "vehiculeMake"-> JsString(""),
      "batteryPercent"->JsString(msg.batteryPercent),
      "temperatureDrone"-> JsString(msg.temperatureDrone),
      "mType"->JsString(msg.mType))
    val record = new ProducerRecord[String,String]("sendCsv",msg.id + "key",JSON.toString())
    prod.send(record)
    println("msg sent")
  }

  def initiateProducer(): Tuple2[KafkaProducer[String,String],SparkSession] = {
    val props: Properties = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    val prod : KafkaProducer[String,String] = new KafkaProducer[String,String](props)

    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
      .getOrCreate()

    (prod,spark)
  }


}
