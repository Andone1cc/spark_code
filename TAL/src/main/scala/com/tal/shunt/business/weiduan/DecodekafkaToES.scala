package com.tal.shunt.business.weiduan

import com.tal.shunt.frame.SparkFrame
import com.tal.shunt.support.{KafkaSupport, ZKSupport}
import com.tal.shunt.util.{DateManager, JsonManager}
import com.tal.shunt.business.weiduan.DecodekafkaToES._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

import scala.collection.Map

/**
  * @note 解密后的kafka数据写入ES
  *       Created by andone1cc on 2018/3/15.
  */
class DecodekafkaToES extends SparkFrame {

  private[this] def create(): StreamingContext = {
    //创建StreamingContext
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    //创建KafkaDirectStream，并且获取每批次的offset
    var offsetRanges = Array[OffsetRange]()
    val logs = KafkaSupport.createDirectStream(ssc, USER_GROUP, MICROEND_DECODE_TOPIC)
      .transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.map(_.value).persist(StorageLevel.MEMORY_ONLY)
      }

    //数据写入ES
    logs.foreachRDD { jsonLogs =>
      val mapLogs = jsonLogs.mapPartitions { jsonLogList =>
        jsonLogList.map { jsonLog =>
          JsonManager.jsonToMap(jsonLog).get.asInstanceOf[Map[String, String]]
        }
      }

      val currentDate = DateManager.getFormatOffsetDate("yyyyMMdd").get
      mapLogs.saveToEs(INDEX.replace("date", currentDate), ES_CONFIG)

      //将本次计算完成的offset写入zk
      ZKSupport.setTopicOffsets(USER_GROUP, offsetRanges)
    }

    ssc.checkpoint(CHECKPOINT_PATH)
    ssc
  }

  override protected def handle(): Unit = {
    val streaming = StreamingContext.getOrCreate(CHECKPOINT_PATH, create)

    streaming.start()
    streaming.awaitTermination()
  }
}


object DecodekafkaToES {

  //ES config
  val ES_CONFIG = Map(
    ConfigurationOptions.ES_NODES -> "10.163.144.173,10.163.144.169,10.81.133.153"
  )

  //ES index
  val INDEX = "microend-decode2-date/decode"

  //用户组
  private val USER_GROUP = "shunt_decodekafka_to_es"

  //解析后的微端日志Topic
  private val MICROEND_DECODE_TOPIC = "microend-decode"

  //checkpoint路径
  private val CHECKPOINT_PATH: String = "hdfs:///user/work/kafka_checkpoint/shunt_decodekafka_to_es"

  def main(args: Array[String]): Unit = {
    new DecodekafkaToES().run()
  }
}