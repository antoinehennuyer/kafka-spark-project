import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import play.api.libs.json._

object main {
  def main(args: Array[String]): Unit = {

    // CODE FOR KAFKA STREAM

    val streamConfig = new Properties()
    streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    streamConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")

    val builder = new StreamsBuilder()
    val text = builder.stream[String,String]("general")
    val alert = text.filter((x,v) => Json.parse(v).\("violation_code").as[JsString].value == "102") // TODO Modify the condition
    //val uppercase = text.mapValues(x => Json.parse(x)).mapValues(x => x.\("ID"))
    //print(uppercase.mapValues(x => print(x)))
    alert.to("alert") // TODO Change to alert topic
    val streams = new KafkaStreams(builder.build(), streamConfig)
    streams.start()


    //streams.close()


  }

}
