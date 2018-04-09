package com.tal.shunt.util

import scala.annotation.tailrec
import scala.collection.Map
import com.tal.shunt.extra.Extra._

import scala.util.{Failure, Success, Try}

/**
  * @note 解码组件
  * @author andone1cc 2018/03/20
  */
object NginxManager extends Log {

  private val NGINX_HEX_FLAG = "\\x"
  private val NGINX_HEX_MIN = 31 //0x1f
  private val NGINX_HEX_MAX = 127 //0x7f

  /**
    * 递归将nginx access log中的十六进制替换数据还原
    *
    * @param source 原始数据
    * @return 解码后的数据
    */
  @tailrec
  def accessLogDecode(source: String): String = {
    val hexCodeStartIndex = source.indexOf(NGINX_HEX_FLAG)

    hexCodeStartIndex match {
      //替换结束
      case index if index == -1 =>
        source

      //正常情况,判断是否能替换为字符,如果可以则全量替换,不可以直接删除
      case index if index + 4 <= source.length =>
        val hexCode = source.substring(index + 2, index + 4)

        var decCode = -1
        Try {
          decCode = Integer.parseInt(hexCode, 16)
        }

        val nextSource = if (decCode != -1) {
          val replaceCode = if (decCode >= NGINX_HEX_MIN && decCode <= NGINX_HEX_MAX) {
            decCode.toChar.toString
          } else {
            ""
          }

          source.replace(NGINX_HEX_FLAG + hexCode, replaceCode)
        } else {
          source.replaceFirstEx(NGINX_HEX_FLAG, "")
        }

        accessLogDecode(nextSource)

      //十六进制不够长直接删除
      case index =>
        source.dropRight(source.length - index)
    }
  }

  /**
    * nginx日志解析
    *
    * @param source 源数据
    * @return 解析后的Map
    */
  def accessLogParse(source: String): Option[Map[String, String]] = {
    //拆解nginx日志
    val sourceList = source.splitEx(" | ")

    //过滤长度小于3的数据
    if (sourceList.length < 3) {
      logWarn(s"source长度小于3: $source")
      return None
    }

    //获取ip,时间,request
    val ip = sourceList(0).trim()
      .nonEmptyExOrElse("-")
    val time = sourceList(1).trim()
      .nonEmptyExOrElse("-")
    val timestamp = DateManager.parseGMT(time)
      .map(_.getTime.toString)
      .getOrElse("-")

    //拆解request
    val requestList = sourceList(2).trim()
      .splitEx(" ")

    //过滤request长度小于2的数据
    if (requestList.length < 2) {
      //logWarn(s"request长度小于2: $source")
      return None
    }

    //获取url,http类型
    val httpType = requestList(0).trim()
      .nonEmptyExOrElse("-")
    val url = requestList(1).trim()
      .nonEmptyExOrElse("-")

    //过滤非get/post类型或者url为空
    if ((httpType != "GET" && httpType != "POST") ||
      url == "-") {
      //logWarn(s"非GET,POST或者url为空: $source")
      return None
    }

    //过滤没有body的post请求
    if (httpType == "POST" && sourceList.length < 6) {
      //logWarn(s"没有body的POST: $source")
      return None
    }

    //过滤空body的POST
    val body = sourceList(5).replace(" ", "")
      .nonEmptyExOrElse("-")
    if (httpType == "POST" && body == "-") {
      //logWarn(s"body为空的POST: $source")
      return None
    }

    //生成结果map
    Some(Map(
      "nginx_ip" -> ip,
      "nginx_time" -> timestamp,
      "nginx_http_type" -> httpType,
      "nginx_url" -> url,
      "nginx_body" -> body
    ))
  }
}

