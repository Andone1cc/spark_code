package com.tal.shunt.frame

import org.apache.spark.sql.SparkSession

/**
  * spark框架基类，负责提供SparkSession实例。
  * Created by andone1cc on 2018/3/13.
  */
trait SparkBaseFrame {

  protected lazy val spark: SparkSession = SparkBaseFrame._spark

}

/**
  * 负责生成SparkSession实例
  */
object SparkBaseFrame {

  private lazy val _spark: SparkSession = create()

  private def setConf(builder: SparkSession.Builder): SparkSession.Builder = {
    //builder.config("spark.debug.maxToStringFields", "255")
    //  .config("spark.streaming.kafka.maxRatePerPartition", "1000")
    builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 优雅的停止, 这样可以保证在driver结束前处理完所有已经接受的数据
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      // 动态控制消费速率
      .config("spark.streaming.backpressure.enabled", "true")
  }

  private def create(): SparkSession = {
    val builder = SparkSession.builder()
    val spark = setConf(builder).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
