package com.tal.shunt.frame

/**
  * 提供SparkCore计算功能，重写handle方法即可。
  * Created by andone1cc on 2018/3/13.
  */
trait SparkFrame extends SparkBaseFrame {

  /**
    * SparkCore执行方法,重写业务逻辑,内部使用spark变量替代SparkSession
    */

  protected def handle(): Unit

  /**
    * 调用该方法启动SparkCore程序
    *
    * @return 结果array数组
    */
  def run(): Unit = {
    handle()
  }
}
