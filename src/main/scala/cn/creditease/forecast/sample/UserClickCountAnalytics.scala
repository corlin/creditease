package cn.creditease.forecast.sample

/**
  * Created by corlinchen on 2017/4/18.
  */
import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object UserClickCountAnalytics {
  def main(args: Array[String]): Unit = {
    var masterUrl = "yarn"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(30))

    // Kafka configurations
    val topics = KafkaProperties.KAFKA_USER_TOPIC.split("\\,").toSet
    println(s"Topics: ${topics}.")

    val brokers = KafkaProperties.KAFKA_ADDR
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "group.id" -> "test-consumer-group",
      "enable.auto.commit".->(false:java.lang.Boolean)
    )


    val clickHashKey = "app::users::click"

    // Create a direct stream

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    );

    /**
    Line ConsumerRecord(topic = user_events, partition = 1, offset = 22092,
    CreateTime = 1492497864996, checksum = 4267521779, serialized key size = -1,
    serialized value size = 107, key = null,
    value = {"uid":"8dfeb5aaafc027d89349ac9a20b3930f","os_type":"Android","click_count":8,"event_time":"1492497864996"}).

      */
    val events = kafkaStream.flatMap(line => {
      println(s"Line ${line}.")
      val data = JSON.parseObject(line.value())
      Some(data)
    })

    // Compute user click times
    val userClicks = events.map(x => (x.getString("uid"), x.getLong("click_count"))).reduceByKey(_ + _)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val jedis = RedisClient.pool.getResource
        partitionOfRecords.foreach(pair => {
          try {
            val uid = pair._1
            val clickCount = pair._2
            jedis.hincrBy(clickHashKey, uid, clickCount)
            println(s"Update uid ${uid} to ${clickCount}.")
          } catch {
            case e: Exception => println("error:" + e)
          }
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
