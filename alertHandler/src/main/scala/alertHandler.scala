import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import play.api.libs.json._
import scala.collection.JavaConverters._

object alertHandler {
  def initAlertConsumer(): KafkaConsumer[String, String] = {
    val consumerConfiguration = new util.Properties()

    consumerConfiguration.put("bootstrap.servers", "localhost:9092")
    consumerConfiguration.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerConfiguration.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerConfiguration.put("auto.offset.reset", "latest")
    consumerConfiguration.put("group.id", "consumer-group")
    //consumerConfiguration.put("max.poll.records", 100)

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](consumerConfiguration)
    consumer.subscribe(util.Arrays.asList("alert"))
    consumer
  }

  def initAlertProducer(): KafkaProducer[String,String] = {
    val producerConfiguration = new util.Properties()
    producerConfiguration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    producerConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    producerConfiguration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    new KafkaProducer[String,String](producerConfiguration)
  }

  def main(args: Array[String]): Unit = {
    val consumer: KafkaConsumer[String, String] = initAlertConsumer()
    val producer: KafkaProducer[String,String] = initAlertProducer()
    while (true) {
      val records = consumer.poll(10000).asScala
      records.foreach { record =>
        println("offset", record.offset())
        producer.send(new ProducerRecord[String,String]("general",record.key(),
          Json.obj("ID"->JsString(Json.parse(record.value()).\("ID").as[JsString].value),
            "location"->JsString(Json.parse(record.value()).\("location").as[JsString].value), "time" ->JsString(Json.parse(record.value()).\("time").as[JsString].value), "violation_code"->JsString("ALERT RESOLVED")).toString()))
      }
    }
    producer.close()
    consumer.close()
    //COMMENT KILL MESSAGE
    // COMMENT VIDER CLOSE
  }
}
