package com.tal.shunt.support

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.Map

/**
  * @note kafka配置文件
  * @author andone1cc 2018/03/13
  */

object KafkaSupport {

  //broker info
  val BROKER_LIST: String = "kafka01:9092,kafka02:9092,kafka03:9092"

  //consumer配置
  private def getConsumerConfig(userGroup: String): Map[String, Object] = {
    Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> BROKER_LIST,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[IntegerSerializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
      ConsumerConfig.GROUP_ID_CONFIG -> userGroup,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> Boolean.box(false)
    )
  }

  //producer配置
  private def getProducerConfig: Map[String, Object] = {
    Map(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> BROKER_LIST,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[IntegerSerializer],
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
      ProducerConfig.COMPRESSION_TYPE_CONFIG -> "lz4",
      ProducerConfig.ACKS_CONFIG -> "0"
    )
  }

  //获取producer
  private lazy val producer = new KafkaProducer[Int, String](getProducerConfig)

  def getProducer: KafkaProducer[Int, String] = {
    producer
  }

  /**
    * 创建一个dstream
    *
    * @param topic topic名称
    * @param ssc   streaming context
    * @return dstream
    */
  def createDirectStream(ssc: StreamingContext, userGroup: String, topic: String): DStream[ConsumerRecord[Int, String]] = {
    val subscribe = ZKSupport.getTopicOffsets(userGroup, topic) match {
      case Some(topicOffsets) =>
        Subscribe[Int, String](
          Seq(topic),
          getConsumerConfig(userGroup),
          topicOffsets
        )
      case None =>
        Subscribe[Int, String](
          Seq(topic),
          getConsumerConfig(userGroup)
        )
    }

    KafkaUtils.createDirectStream[Int, String](
      ssc,
      PreferConsistent,
      subscribe
    )
  }
}
