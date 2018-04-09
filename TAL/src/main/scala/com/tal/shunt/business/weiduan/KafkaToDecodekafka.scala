package com.tal.shunt.business.weiduan

import com.tal.shunt.frame.SparkFrame
import com.tal.shunt.support.{KafkaSupport, NginxSupport, ZKSupport}
import com.tal.shunt.util._
import com.tal.shunt.business.weiduan.KafkaToDecodekafka._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @note 实时解析微端日志
  *       Created by andone1cc on 2018/3/15.
  */
class KafkaToDecodekafka extends SparkFrame {

  private[this] def create(): StreamingContext = {
    //创建StreamingContext
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    //创建KafkaDirectStream，并且获取每批次的offset
    var offsetRanges = Array[OffsetRange]()
    val logs = KafkaSupport.createDirectStream(ssc, USER_GROUP, MICROEND_TOPIC)
      .transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.map(_.value).persist(StorageLevel.MEMORY_ONLY)
      }

    val jsonMaps = logs.mapPartitions { logList =>
      //解析nginx log
      val parseLogList = logList.map(NginxSupport.accessLogParse)
        .filter(_.nonEmpty).map(_.get)

      //解析get/post请求
      val decodeMapList = parseLogList.map { logMap =>
        if (logMap("nginx_http_type") == "GET") {
          val url = logMap("nginx_url")
          logMap ++ HttpManager.queryParse(HttpManager.getQuery(url)) - "nginx_url"
        } else {
          val body = logMap("nginx_body")
          logMap ++ HttpManager.bodyParse(body) - "nginx_body"
        }
      }

      //规范日志
      val filterMapList = decodeMapList.map(LogFormat.format)

      //将map转为json字符串
      filterMapList.map(decodeMap => JsonManager.mapToJson(decodeMap))
        .filter(_.nonEmpty).map(_.get)
    }

    //数据写入
    jsonMaps.foreachRDD { rdd =>
      //数据写入kafka
      rdd.foreachPartition { partition =>
        val producer = KafkaSupport.getProducer
        partition.foreach { element =>
          val message = new ProducerRecord[Int, String](MICROEND_DECODE_TOPIC, element)
          producer.send(message)
        }
      }

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

object KafkaToDecodekafka {

  //用户组
  private val USER_GROUP = "shunt_kafka_to_decodekafka"

  //微端日志Topic
  private val MICROEND_TOPIC = "microend"

  //解析后的微端日志Topic
  private val MICROEND_DECODE_TOPIC = "microend-decode"

  //checkpoint路径
  private val CHECKPOINT_PATH: String = "hdfs:///user/work/kafka_checkpoint/shunt_kafka_to_decodekafka"

  def main(args: Array[String]): Unit = {
    new KafkaToDecodekafka().run()
  }
}
