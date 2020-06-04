import java.util
import java.util.concurrent.TimeUnit

import scala.util.Random
import Random.nextInt
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import akka.actor.ActorSystem
import play.api.libs.json.{JsString, Json}

object alertHandler {
  def initAlertConsumer(): KafkaConsumer[String, String] = {
    val consumerConfiguration = new util.Properties()

    consumerConfiguration.put("bootstrap.servers", "localhost:9092")
    consumerConfiguration.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerConfiguration.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerConfiguration.put("auto.offset.reset", "latest")
    consumerConfiguration.put("group.id", "consumer-group")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerConfiguration)
    consumer.subscribe(util.Arrays.asList("alert"))
    consumer
  }

  def chooseSolve(): String = {
    val rand = nextInt(100)
    if (rand < 50) {
      val r = 1 + nextInt(99)
      print("Alert detected and 1 police officer changed it to the code: " + r.toString() + "\n")
      r.toString()
    }
    else{
      
      print("Alert detected and 1 police officer changed it to the code: 101\n")
      "101"
    }
  }
  def initAlertProducer(): KafkaProducer[String,String] = {
    val producerConfiguration = new util.Properties()
    producerConfiguration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    producerConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    producerConfiguration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    new KafkaProducer[String,String](producerConfiguration)
  }

  def sendRecords(consumer: KafkaConsumer[String, String], producer: KafkaProducer[String,String]) = {
    val records = consumer.poll(5000).asScala
    records.foreach { record =>
      println("offset", record.offset())
      val res = chooseSolve()
      producer.send(new ProducerRecord[String,String]("general",record.key(),
        Json.obj("ID"->JsString(Json.parse(record.value()).\("ID").as[JsString].value),
          "location"->JsString(Json.parse(record.value()).\("location").as[JsString].value),
          "time" ->JsString(Json.parse(record.value()).\("time").as[JsString].value),
          "violation_code"->JsString(res),
          "state"->JsString(Json.parse(record.value()).\("state").as[JsString].value),
          "vehiculeMake"->JsString(Json.parse(record.value()).\("vehiculeMake").as[JsString].value),
          "batteryPercent"->JsString(Json.parse(record.value()).\("batteryPercent").as[JsString].value),
          "temperatureDrone"->JsString(Json.parse(record.value()).\("temperatureDrone").as[JsString].value),
          "mType"->JsString(Json.parse(record.value()).\("mType").as[JsString].value),
          "imageId"->JsString(Json.parse(record.value()).\("imageId").as[JsString].value)
        ).toString()))
    }
  }

  def main(args: Array[String]): Unit = {
    val consumer: KafkaConsumer[String, String] = initAlertConsumer()
    val producer: KafkaProducer[String,String] = initAlertProducer()

    val actorSystem = ActorSystem()
    val scheduler = actorSystem.scheduler
    val task = new Runnable { def run() { sendRecords(consumer, producer) } }
    implicit val executor = actorSystem.dispatcher

    scheduler.schedule(
      initialDelay = Duration(0, TimeUnit.SECONDS),
      interval = Duration(10, TimeUnit.SECONDS),
      runnable = task)

    producer.close()
  }
}
