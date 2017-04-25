package cn.creditease.forecast.sample

/**
  * Created by corlinchen on 2017/4/18.
  */
object KafkaProperties {
  val REDIS_SERVER: String = "devmac"
  val REDIS_PORT: Int = 6379

  val KAFKA_SERVER: String = "devmac"
  val KAFKA_ADDR: String = KAFKA_SERVER + ":9092"
  val KAFKA_USER_TOPIC: String = "user_events"
  val KAFKA_RECO_TOPIC: String = "reco6"


}
