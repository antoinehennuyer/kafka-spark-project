package core


//import scala.concurrent._
//import ExecutionContext.Implicits.global
import utils.MessageUtils
import utils.MessageUtils._
import scala.util.Random
import Random.nextInt
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object CreateMessage {
  var violation = 0
  var alert = 0
  var message = 0

  def CreateDronesMessages(nbrDrone: Int, nbrMessage: Int): Unit = {
    val res = Stream.continually(Random.alphanumeric.filter(_.isDigit).take(5).mkString).take(nbrDrone)
    res.foreach(elt => {
      RandomMessage(nbrMessage, elt) // TODO: Make it async
    })

  }

  def RandomMessage(nbr: Int, idDrone: String): Any = {
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
            val newMessage = MessageGenerate(idDrone, "Paris", date, "alert")
            println(newMessage)
            alert += 1
          }
          else {
            val newMessage = MessageGenerate(idDrone, "Paris", date, "violation")
            println(newMessage)
            violation += 1
          }
        }
        else {
          val newMessage = MessageGenerate(idDrone, "Paris", date, "regularly message")
          println(newMessage)
          message += 1
        }
        RandomMessage(nbr - 1, idDrone)
      }
    }
  }

  def MessageGenerate(id: String, loc: String, time: String, vioCode: String): Message = {
    MessageUtils.Message(id, loc, time, vioCode)
  }
}
