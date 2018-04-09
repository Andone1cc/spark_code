package com.tal.shunt.util

import java.sql.Timestamp
import java.util.{Calendar, Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

import scala.util.{Failure, Success, Try}

/**
  * @note 日期处理组件
  * @author andone1cc 2018/03/13
  */
object DateManager extends Log {

  /**
    * 将long类型时间戳转化为java.sql.Timestamp类型
    * @param timestamp 时间戳
    * @return Option类型的Timestamp
    */
  def getTimestamp(timestamp: Long): Option[Timestamp] = {
    Try {
      new Timestamp(timestamp)
    } match {
      case Success(date) =>
        Some(date)
      case Failure(_) =>
        logWarn(s"$timestamp getDate fail")
        None
    }
  }

  /**
    * 将long类型时间戳转化为Date类型
    * @param timestamp 时间戳
    * @return Option类型的date
    */
  def getDate(timestamp: Long): Option[Date] = {
    Try {
      new Date(timestamp)
    } match {
      case Success(date) =>
        Some(date)
      case Failure(_) =>
        logWarn(s"$timestamp getDate fail")
        None
    }
  }

  /**
    * 将long类型时间戳转化为指定格式的String类型
    * @param timestamp 时间戳
    * @param format    格式
    * @return Option类型的str
    **/
  def getFormatDate(timestamp: Long, format: String): Option[String] = {
    val dateFormat = FastDateFormat.getInstance(format, Locale.US)
    Try {
      dateFormat.format(new Date(timestamp).getTime)
    } match {
      case Success(date) =>
        Some(date)
      case Failure(_) =>
        logWarn(s"$timestamp getDate fail")
        None
    }
  }

  /**
    * 按照指定格式返回当前时间的偏移
    * @param format 指定的格式
    * @param offset 指定的前后偏移量
    * @return 当前时间
    */
  def getFormatOffsetDate(format: String, offset: Int = 0): Option[String] = {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, offset)

    val dateFormat = FastDateFormat.getInstance(format, Locale.US)
    Try {
      dateFormat.format(calendar.getTime)
    } match {
      case Success(date) =>
        Some(date)
      case Failure(_) =>
        logWarn(s"$format format fail")
        None
    }
  }

  /**
    * 按照指定格式解析时间
    * @param time   需要解析的时间字符串
    * @param format 指定的格式
    * @return Date
    */
  def parse(time: String, format: String): Option[Date] = {
    val dateFormat = FastDateFormat.getInstance(format, Locale.US)
    Try {
      dateFormat.parse(time)
    } match {
      case Success(date) =>
        Some(date)
      case Failure(_) =>
        logWarn(s"$time $format parse fail")
        None
    }
  }

  /**
    * 按照指定格式解析时间
    * @param time   需要解析的时间字符串
    * @param format 指定的格式
    * @return Timestamp
    */
  def parseTimestamp(time: String, format: String): Option[Timestamp] = {
    val dateFormat = FastDateFormat.getInstance(format, Locale.US)
    Try {
      new Timestamp(dateFormat.parse(time).getTime)
    } match {
      case Success(timestamp) =>
        Some(timestamp)
      case Failure(_) =>
        logWarn(s"$time $format parseTimestamp fail")
        None
    }
  }

  /**
    * 按照指定格式解析时间
    * @param time   需要解析的时间字符串
    * @param format 指定的格式
    * @return java.sql.Date
    */
  def parseSqlDate(time: String, format: String): Option[java.sql.Date] = {
    val dateFormat = FastDateFormat.getInstance(format, Locale.US)
    Try {
      new java.sql.Date(dateFormat.parse(time).getTime)
    } match {
      case Success(sqlDate) =>
        Some(sqlDate)
      case Failure(_) =>
        logWarn(s"$time $format parseSqlDate fail")
        None
    }
  }

  /**
    * 解析格林威治时间
    * @param time 需要解析的时间
    * @return 时间戳
    */
  def parseGMT(time: String): Option[Date] = {
    parse(time, "dd/MMM/yyyy:hh:mm:ss Z")
  }
}

