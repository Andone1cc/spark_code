package com.tal.shunt.support

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.ZKGroupTopicDirs

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.util.Try

/**
  * @note zk帮助类
  * @author andone1cc 2018/03/13
  */
object ZKSupport {

  //zookeeper集群host
  private val ZK_HOST = "zookeeper01:2181,zookeeper02:2181,zookeeper03:2181"

  //获取producer
  private lazy val zkClient = new ZkClient(
    ZK_HOST,
    Integer.MAX_VALUE,
    10000,
    new ZkSerializer {
      override def serialize(data: Object): Array[Byte] = {
        data.asInstanceOf[String].getBytes("UTF-8")
      }

      override def deserialize(bytes: Array[Byte]): Object = {
        if (bytes == null) {
          null
        } else {
          new String(bytes, "UTF-8")
        }
      }
    }
  )

  private def getZKClient: ZkClient = {
    zkClient
  }

  /**
    * 获取topic对应所有partition的offset
    *
    * @param topic topic的名字
    * @return partitionID -> offset的Option[Map], 如果失败返回None
    */
  def getTopicOffsets(userGroup: String, topic: String): Option[Map[TopicPartition, Long]] = {
    //获取第一个broker的host和port
    val firstBrokerHost = KafkaSupport.BROKER_LIST.split(",")(0).split(":")(0)
    val firstBrokerPort = KafkaSupport.BROKER_LIST.split(",")(1).split(":")(1).toInt

    //通过一个Broker获取该topic中每个partition对应的leader信息
    val getLeaderRequest = new TopicMetadataRequest(Seq(topic), 0)
    val getLeaderConsumer = new SimpleConsumer(firstBrokerHost, firstBrokerPort, 10000, 10000, "get_leader")
    val getLeaderResult = getLeaderConsumer.send(getLeaderRequest)
    val leaderInfo = getLeaderResult.topicsMetadata.headOption match {
      case Some(topicMetadata) =>
        topicMetadata.partitionsMetadata.map { partitionMetadata =>
          (partitionMetadata.partitionId, partitionMetadata.leader.get.host)
        }.toMap[Int, String]
      case None =>
        return None
    }

    //创建ZK客户端
    val zkClient = getZKClient

    //获取zk中的topic相关信息
    val topicDirs = new ZKGroupTopicDirs(userGroup, topic)
    val partitionIDs = 0 until zkClient.countChildren(topicDirs.consumerOffsetDir)

    //获取partition对应的offsets
    val topicOffsetList = partitionIDs.map { id =>
      //从zk中获取offset
      val path = topicDirs.consumerOffsetDir + "/" + id
      if (!zkClient.exists(path)) {
        return None
      }
      val zkOffset = zkClient.readData[String](path).toLong
      val topicAndPartition = TopicAndPartition(topic, id)

      //从leader中获取offset
      val getEarliesRequest = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
      val getEarliesConsumer = new SimpleConsumer(leaderInfo(id), firstBrokerPort, 10000, 10000, "get_earliest")
      val earliestOffsets = getEarliesConsumer.getOffsetsBefore(getEarliesRequest).partitionErrorAndOffsets(topicAndPartition).offsets

      //如果zk中存储的offset已经被kafka清理了,则使用kafka的最老offset作为起始位置
      var partitionOffset = zkOffset
      if (earliestOffsets.nonEmpty && zkOffset < earliestOffsets.head) {
        partitionOffset = earliestOffsets.head
      }
      new TopicPartition(topic, id) -> partitionOffset
    }
    Some(topicOffsetList.toMap)
  }

  /**
    * 向zk中写入offset,该函数一般调用于任务处理结束后
    *
    * @param userGroup    用户组
    * @param offsetRanges partition的offset信息集合
    */
  def setTopicOffsets(userGroup: String, offsetRanges: Array[OffsetRange]): Unit = {
    //创建zk客户端
    val zkClient = getZKClient

    //获取zk中topic的相关信息
    val topicDirs = new ZKGroupTopicDirs(userGroup, offsetRanges.head.topic)

    //写入topic对应的offsets
    offsetRanges.foreach { offsetRange =>
      //获取offset信息
      val partitionID = offsetRange.partition
      val offset = offsetRange.untilOffset

      //获取partition对应的zk路径
      val path = topicDirs.consumerOffsetDir + "/" + partitionID

      //向zk中写入offset
      var retryFlag = 0
      while (retryFlag >= 0) {
        Try {
          zkClient.writeData(path, offset.toString)
          retryFlag = -1
        } recover {
          //如果是由于没有zk文件夹导致的,则直接创建zk文件夹后重试
          case _: Throwable if !zkClient.exists(path) =>
            zkClient.createPersistent(path, true)
          //其他情况则认为zk连接异常,重试次数加一
          case _ if retryFlag <= 10 =>
            retryFlag += 1
            Thread.sleep(100)
          //超过10次则认为写入zk失败,退出任务
          case _ =>
            println(s"zookeeper写入元数据失败, offsetRanges: $offsetRanges")
            System.exit(1)
        }
      }
    }
  }

}
