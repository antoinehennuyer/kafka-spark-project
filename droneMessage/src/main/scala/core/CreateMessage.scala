package core


//import scala.concurrent._
//import ExecutionContext.Implicits.global
import utils.MessageUtils
import utils.MessageUtils._
//import scala.util.Random
//import Random.nextInt
//import java.time.LocalDateTime
//import java.time.format.DateTimeFormatter

object CreateMessage {


  def MessageGenerate(id: String, loc: String, time: String, vioCode: String): Message = {
    MessageUtils.Message(id, loc, time, vioCode)
  }
}
