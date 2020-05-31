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

  var addressList = List(
    "29 Main Ave. Ocean Springs, MS 39564",
    "8653 Ridge Court, Oceanside, NY 11572",
    "8205 Greenrose Ave, Downers Grove, IL 60515",
    "8945 W. Ridge St, East Hartford, CT 06118",
    "28 Mayfield Street, West Hempstead, NY 11552",
    "784 Wild Rose Drive, Mcdonough, GA 30252"
  )



  def CreateDronesMessages(nbrDrone: Int, nbrMessage: Int): Any = {
    val prod = initiateProducer()
    val listDrone = Stream.continually(Random.alphanumeric.filter(_.isDigit).take(5).mkString).take(nbrDrone)


    RandomMessage(nbrMessage, listDrone, prod)

//    listDrone.par.foreach(elt => {
//      RandomMessage2(nbrMessage, elt, prod) // TODO: Make it async
//    })
    prod.close()

  }

  def RandomMessage(nbr: Int, idsDrone: Stream[String], prod: KafkaProducer[String,String]): Any = {
    nbr match {
      case 0 => {
        println("violation: ", violation)
        println("alert: ", alert)
        println("message: ", message)
      }
      case _ => {
        val date = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").format(LocalDateTime.now())
        val randomType = nextInt(100)
        val idDrone = idsDrone(nextInt(idsDrone.length))
        val loc = addressList(nextInt(addressList.length))
        val battery = (nextInt(100) + 1).toString
        val temp = (nextInt(31) + 20).toString
        if (randomType < 25) {
          val typeAlert = nextInt(100)
          if (typeAlert == 0) {
            MessageGenerate(idDrone, loc, date, "102", battery, temp, prod)
            alert += 1
          }
          else {
            val viola = (nextInt(100) + 1).toString
            MessageGenerate(idDrone, loc, date, viola, battery, temp, prod)
            violation += 1
          }
        }
        else {
          MessageGenerate(idDrone, loc, date, "101", battery, temp, prod)
          message += 1
        }
        Thread.sleep(1000)
        RandomMessage(nbr - 1, idsDrone, prod)
      }
    }
  }

  def RandomMessage2(nbr: Int, idDrone: String, prod: KafkaProducer[String,String]): Any = {
    nbr match {
      case 0 => {
        println("violation: ", violation)
        println("alert: ", alert)
        println("message: ", message)
      }
      case _ => {
        val date = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").format(LocalDateTime.now())
        val randomType = nextInt(100)
        val loc = addressList(nextInt(addressList.length))
        val battery = (nextInt(100) + 1).toString
        val temp = (nextInt(31) + 20).toString
        if (randomType < 25) {
          val typeAlert = nextInt(100)
          if (typeAlert == 0) {
            MessageGenerate(idDrone, loc, date, "102", battery, temp, prod)
            alert += 1
          }
          else {
            val viola = (nextInt(100) + 1).toString
            MessageGenerate(idDrone, loc, date, viola, battery, temp, prod)
            violation += 1
          }
        }
        else {
          MessageGenerate(idDrone, loc, date, "101", battery, temp, prod)
          message += 1
        }
        Thread.sleep(1000)
        RandomMessage2(nbr - 1, idDrone, prod)
      }
    }
  }

  def MessageGenerate(id: String, loc: String, time: String, vioCode: String, battery: String, temp: String, prod: KafkaProducer[String,String]): Any = {
    val msg = MessageUtils.Message(id, loc, time, vioCode, "", "", battery, temp, "DRO")
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
    // val record = new ProducerRecord[String,String]("general",msg.id + "key",JSON.toString())
    // prod.send(record)
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
