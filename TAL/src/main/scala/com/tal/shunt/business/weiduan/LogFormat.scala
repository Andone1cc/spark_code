package com.tal.shunt.business.weiduan

import java.net.{URL, URLDecoder}
import java.util

import com.tal.shunt.business.Default._
import com.tal.shunt.util.{ConfManager, DateManager, JsonManager}

import scala.collection.Map
import scala.util.Try
import scala.collection.JavaConverters._

/**
  * @note 日志格式化
  * @author andone1cc 2018/03/15
  */
object LogFormat {
  //获取配置文件
  private lazy val confFile = Thread.currentThread().getContextClassLoader.getResourceAsStream("project.properties")
  private lazy val conf = ConfManager(confFile)

  //映射关系Map初始化
  private lazy val institutionMatchMap = JsonManager.jsonToMap(conf.getString("institution.match.info")).get
  private lazy val platformMatchMap = JsonManager.jsonToMap(conf.getString("platform.match.info")).get
  private lazy val usertypeMatchMap = JsonManager.jsonToMap(conf.getString("usertype.match.info")).get

  /**
    * 针对业务日志上报规范化
    *
    * @param logMap 日志map
    * @return 规范化后的日志map
    */
  def format(logMap: Map[String, Any]): Map[String, String] = {
    //过滤空格,并创建可变map
    val formattedMap = new util.HashMap[String, String]()
    logMap.map {
      case (key, value: String) =>
        var trimValue = value.asInstanceOf[String].trim()
        if (trimValue == "") {
          trimValue = STRING_DEFAULT
        }
        formattedMap.put(key, trimValue)
      case (key, value) =>
        formattedMap.put(key, value.toString)
    }

    //规范用户id
    var uid = formattedMap.getOrDefault("uid", STRING_DEFAULT) match {
      case STRING_DEFAULT =>
        var imei = STRING_DEFAULT
        Try {
          imei = formattedMap.get("guid").toString
        }

        imei
      case id =>
        id
    }
    if (uid.length > 100) { //限制uid长度
      uid = uid.substring(0, 100)
    }
    formattedMap.put("uid", uid)

    //规范机构id
    val projectID = formattedMap.getOrDefault("project_id", STRING_DEFAULT)
    val institutionID = institutionMatchMap.getOrElse(projectID, INT_DEFAULT.toString).asInstanceOf[String]
    formattedMap.put("institution_id", institutionID)

    //规范平台类型
    val cmdID = formattedMap.getOrDefault("cmd_id", STRING_DEFAULT)
    formattedMap.put("cmd_id", cmdID)
    val platform = platformMatchMap.getOrElse(cmdID, INT_DEFAULT.toString).asInstanceOf[String]
    formattedMap.put("platform", platform)

    //规范播放类型
    var playType = formattedMap.getOrDefault("videotype", STRING_DEFAULT) match {
      case "live" | "zhibo" =>
        "1"
      case "video" | "dianbo" | "vod" =>
        "2"
      case _ =>
        INT_DEFAULT.toString
    }
    //播放类型补充
    val action = formattedMap.getOrDefault("action", STRING_DEFAULT)
    if (action == "startpublish" || action == "stoppublish" || action == "uploadspeed" ||
      institutionID == "1") {
      playType = "1"
    }
    formattedMap.put("play_type", playType)

    //规范stream_id
    val streamID = formattedMap.getOrDefault("streamid", STRING_DEFAULT) match {
      case "-" if formattedMap.containsKey("url") =>
        var sid = "-"
        Try {
          val decodeURL = URLDecoder.decode(formattedMap.get("url"), "UTF8")
          val path = new URL(decodeURL.replace("rtmp", "http")).getPath
          val pathList = path.split("/")
          if (pathList.length > 1) {
            sid = pathList.last
          }
        }
        //截取点后字符
        val postfixIndex = sid.indexOf(".")
        if (postfixIndex != -1) {
          sid = sid.substring(0, postfixIndex)
        }

        sid
      case sid =>
        sid
    }
    formattedMap.put("stream_id", streamID)

    //规范role
    val realRole = formattedMap.getOrDefault("role", STRING_DEFAULT) match {
      case role@("t" | "a" | "s") =>
        role
      case _ =>
        val usertypeKey = formattedMap.getOrDefault("project_id", STRING_DEFAULT) +
          formattedMap.getOrDefault("cmd_id", STRING_DEFAULT)
        usertypeMatchMap.getOrElse(usertypeKey, STRING_DEFAULT).asInstanceOf[String]
    }
    formattedMap.put("role", realRole)

    //添加es时间
    val esTime = DateManager.getFormatDate(formattedMap.get("nginx_time").toLong, "yyyy-MM-dd'T'HH:mm:ss.SSSZ").get
    formattedMap.put("es_time", esTime)

    formattedMap.asScala
  }
}
