package core

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
//import play.api.libs.json.{JsString, Json}
import utils.MessageUtils
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import play.api.libs.json._
import scala.util.Random
import scala.io.Source

object send_nypd {


  def CreateNypdMessages(): Any = {
    val prod = initiateProducer()
    val idx = List(0,1,2)
    //val nypdCsv = spark.read.option("header", "true").csv("Parking_Violations_Issued_-_Fiscal_Year_2017.csv").toDF()
    val nypdCsv = Source.fromFile("Parking_Violations_Issued_-_Fiscal_Year_2017.csv").getLines.map(_.split(",").toList)
    //nypdCsv.map(x => List(x(25),x(5),x(6),x(8)))
    nypdCsv.foreach(line => MessageGenerate("NA",line(24),line(4),line(5),line(2),line(7),prod))
    prod.close()
  }

  def MessageGenerate(id: String, loc: String, time: String, vioCode: String,state: String,v_make:String, prod: KafkaProducer[String,String]): Any = {
    val msg = MessageUtils.Message(id, loc, time, vioCode,state, v_make, "","","NYP")
    sendMessage(msg, prod)
  }

  def sendMessage(msg : MessageUtils.Message, prod: KafkaProducer[String,String]): Any = {
    val JSON = Json.obj("ID"->JsString(msg.id), "location"->JsString(msg.location),
      "time"->JsString(msg.time), "violation_code"->JsString(msg.violationCode),
      "state"->JsString(msg.state), "vehiculeMake"-> JsString(msg.vehiculeMake),
      "batteryPercent"->JsString(msg.batteryPercent),
      "temperatureDrone"-> JsString(msg.temperatureDrone),
      "mType"->JsString(msg.mType))
    val record = new ProducerRecord[String,String]("sendCsv",msg.id + "key",JSON.toString())
    prod.send(record)
  }

  def initiateProducer(): KafkaProducer[String,String] = {
    val props: Properties = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    val prod : KafkaProducer[String,String] = new KafkaProducer[String,String](props)
    prod
  }

}
