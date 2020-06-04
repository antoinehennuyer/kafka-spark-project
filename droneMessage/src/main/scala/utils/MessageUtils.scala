package utils

object MessageUtils {
  case class Message (
                     id : String,
                     location: String,
                     time: String,
                     violationCode: String,
                     state: String,
                     vehiculeMake: String,
                     batteryPercent: String,
                     temperatureDrone: String,
                     mType: String,
                     imageId: String,
                     )


}
