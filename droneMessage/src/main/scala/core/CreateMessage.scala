package core


//import scala.concurrent._
//import ExecutionContext.Implicits.global
import utils.MessageUtils
import utils.MessageUtils._
import scala.util.Random
import Random.nextInt
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import play.api.libs.json._

object CreateMessage {
  var violation = 0
  var alert = 0
  var message = 0

  def CreateDronesMessages(nbrDrone: Int, nbrMessage: Int): Any = {
    val prod = initiateProducer()
    val res = Stream.continually(Random.alphanumeric.filter(_.isDigit).take(5).mkString).take(nbrDrone)
    res.foreach(elt => {
      RandomMessage(nbrMessage, elt, prod) // TODO: Make it async
    })
    prod.close()

  }

  def RandomMessage(nbr: Int, idDrone: String, prod: KafkaProducer[String,String]): Any = {
    nbr match {
      case 0 => {
        println("violation: ", violation)
        println("alert: ", alert)
        println("message: ", message)
      }
      case _ => {
        val date = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").format(LocalDateTime.now())
//        Thread.sleep(5000)
        val randomType = nextInt(100)
        if (randomType < 25) {
          val typeAlert = nextInt(100)
          if (typeAlert == 0) {
            MessageGenerate(idDrone, "Paris", date, "alert", prod)
            alert += 1
          }
          else {
            MessageGenerate(idDrone, "Paris", date, "violation", prod)
            violation += 1
          }
        }
        else {
          MessageGenerate(idDrone, "Paris", date, "regular message", prod)
          message += 1
        }
        RandomMessage(nbr - 1, idDrone, prod)
      }
    }
  }

  def MessageGenerate(id: String, loc: String, time: String, vioCode: String, prod: KafkaProducer[String,String]): Any = {
    val msg = MessageUtils.Message(id, loc, time, vioCode)
    println(msg)
    sendMessage(msg, prod)
  }
  def sendMessage(msg : MessageUtils.Message, prod: KafkaProducer[String,String]): Any = {
    val JSON = Json.obj("ID"->JsString(msg.id), "location"->JsString(msg.location), "time"->JsString(msg.time), "violation_code"->JsString(msg.violationCode))
    val record = new ProducerRecord[String,String]("general",msg.id + "key",JSON.toString())
    prod.send(record)
    println("msg sent")
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
