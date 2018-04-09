package com.tal.shunt.util

import java.io.InputStream
import java.util.Properties

import scala.util.{Failure, Success, Try}

/**
  * @note 配置组件,使用绝对路径,一个配置文件对应一个ConfManager对象
  * @author andone1cc 2018/03/13
  */
final class ConfManager(fileStream: InputStream) extends Log {

  private[this] lazy val _prop = create()

  /**
    * 初始化改manager，将指定的properties文件存入prop成员中
    * 创建对象时调用
    */
  private def create(): Properties = {
    val prop = new Properties()
    Try {
      prop.load(fileStream)
    } recover {
      case ex: Throwable =>
        logError("init error", ex)
    }

    prop
  }

  /**
    * 获取指定key对应的value
    *
    * @param key 需要的key
    * @return key对应的value
    */
  def getString(key: String): String = {
    _prop.getProperty(key)
  }

  /**
    * 获取整数类型的配置项
    *
    * @param key 需要的key
    * @return key对应int类型的value 默认返回0
    */
  def getInt(key: String): Int = {
    Try {
      getString(key).toInt
    } match {
      case Success(value) =>
        value
      case Failure(ex) =>
        logError("getInt error", ex)
        ConfManager.DEFAULT_INT
    }
  }

  /**
    * 获取布尔类型的配置项
    *
    * @param key 需要的key
    * @return key对应boolean类型的value 默认返回false
    */
  def getBoolean(key: String): Boolean = {
    Try {
      getString(key).toBoolean
    } match {
      case Success(value) =>
        value
      case Failure(ex) =>
        logError("getBoolean error", ex)
        ConfManager.DEFAULT_BOOLEAN
    }
  }

  /**
    * 获取Long类型的配置项
    *
    * @param key 需要的key
    * @return key对应long类型的value 默认返回0L
    */
  def getLong(key: String): Long = {
    Try {
      getString(key).toLong
    } match {
      case Success(value) =>
        value
      case Failure(ex) =>
        logError("getLong error", ex)
        ConfManager.DEFAULT_LONG
    }
  }
}

/* *
 * 配置文件相关常量
 */
object ConfManager {

  val DEFAULT_INT = 0

  val DEFAULT_BOOLEAN = false

  val DEFAULT_LONG = 0L

  def apply(fileStream: InputStream): ConfManager = {
    new ConfManager(fileStream)
  }
}
