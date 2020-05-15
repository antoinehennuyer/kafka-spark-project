import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConverters._

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

  def initAlertProducer(): KafkaProducer[String,String] = {
    val producerConfiguration = new util.Properties()
    producerConfiguration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    producerConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    producerConfiguration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    new KafkaProducer[String,String](producerConfiguration)
  }

  def main(args: Array[String]): Unit = {
    val consumer: KafkaConsumer[String, String] = initAlertConsumer()

    //TEST CONSUMER
    val records = consumer.poll(10000).asScala

    for (data <- records.iterator) {
      println("Data", data.value())
    }

    /*val producer: KafkaProducer[String,String] = initAlertProducer()
    val record = new ProducerRecord[String,String]("alert","key","value")
    producer.send(record)
    producer.close()*/
  }
}