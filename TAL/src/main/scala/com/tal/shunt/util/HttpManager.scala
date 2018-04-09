package com.tal.shunt.util

import java.net.ServerSocket

import com.tal.shunt.extra.Extra._
import com.tal.shunt.support.NginxSupport
import com.tal.shunt.util.aes.AESManager

import scala.io.Source
import scala.util.Try
import scala.collection.Map

/**
  * @note note
  * @author andone1cc 2018/03/13
  */
object HttpManager extends Log {

  /**
    * 随机获取一个可用port
    *
    * @return port
    */
  def getFreePort: Int = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()

    port
  }

  /**
    * 根据url获取返回的数据
    *
    * @param url url地址
    * @return String类型返回数据
    */
  def get(url: String): String = {
    var body = ""
    Try {
      body = Source.fromURL(url, "UTF-8").mkString
    } recover {
      case ex: Throwable =>
        println(ex)
    }

    body
  }

  /**
    * 解析get请求中的url参数信息
    *
    * @param query query
    * @return 解析后的map
    */
  def queryParse(query: String): Map[String, String] = {
    query.splitEx("&").map { element =>
      val kv = element.splitEx("=", 2)
      kv.length match {
        case 0 =>
          ("-", "-")
        case 1 =>
          (kv(0).nonEmptyExOrElse("-"), "-")
        case 2 =>
          (kv(0).nonEmptyExOrElse("-"), kv(1).nonEmptyExOrElse("-"))
      }
    }.toMap
  }

  /**
    * 解析post请求body数据,如果data中需要aes解码则解码
    *
    * @param body body体
    * @return decode后的Map
    */
  def bodyParse(body: String): Map[String, Any] = {
    val decodeBody = NginxSupport.accessLogDecode(body)

    val parseMap = if (decodeBody.length == 0) {
      Map[String, Any]()
    } else if (decodeBody.charAt(0) == '{') {
      JsonManager.jsonToMap(decodeBody) match {
        case Some(jsonMap) =>
          jsonMap
        case None =>
          /*
                    logInfo(s"body to json error: { \n" +
                      s"body: $body \n" +
                      s"decodeBody: $decodeBody \n" +
                      s"}")
          */

          Map[String, Any]()
      }
    } else {
      queryParse(getQuery(decodeBody))
    }

    val dataMap = parseMap.getOrElse("data", "") match {
      case "" =>
        Map[String, Any]()
      case aesSource: String =>
        val aesData = aesSource.replace("\n", "")
        val aesDecodeData = AESManager.decrypt(aesData)
        if (aesDecodeData == null) {
          /*          logInfo(s"aes error: { \n" +
                      s"body: $body \n" +
                      s"decodeBody: $decodeBody \n" +
                      s"aes: $aesData \n" +
                      s"}")*/

          Map[String, Any]()
        } else {
          JsonManager.jsonToMap(aesDecodeData) match {
            case Some(decodeMap) =>
              decodeMap
            case None =>
              /*              logInfo(s"data to json error: { \n" +
                              s"body: $body \n" +
                              s"decodeBody: $decodeBody \n" +
                              s"aes: $aesData \n" +
                              s"decodeAes: $aesDecodeData \n" +
                              s"}")*/

              Map[String, Any]()
          }
        }
      case other: Map[_, _] =>
        other.asInstanceOf[Map[String, Any]]
    }

    parseMap ++ dataMap - "data"
  }

  def getQuery(url: String): String = {
    val decodeUrl = NginxSupport.accessLogDecode(url)
    //该处直接返回是对应post请求中body体为key=value&形式的处理
    if (decodeUrl.indexOf("?") == -1 || decodeUrl.splitEx("?", 2).length != 2) {
      decodeUrl
    } else {
      decodeUrl.splitEx("?", 2)(1)
    }
  }
}

