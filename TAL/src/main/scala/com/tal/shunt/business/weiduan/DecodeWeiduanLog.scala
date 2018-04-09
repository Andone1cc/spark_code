package com.tal.shunt.business.weiduan

import java.net.{URL, URLDecoder}

import com.tal.shunt.frame.SparkFrame
import com.tal.shunt.util._
import com.tal.shunt.business.Default._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders.kryo

import scala.collection.{Map, mutable}
import scala.util.Try

/**
  * @note 解码微端日志
  *       Created by andone1cc on 2018/3/20.
  */
class DecodeWeiduanLog(logPath: String, decodePath: String, conf: ConfManager) extends SparkFrame {

  override protected def handle(): Unit = {
    import spark.implicits._
    implicit val mapStringAnyEncoder: Encoder[Map[String, Any]] = kryo
    implicit val mapStringStringEncoder: Encoder[Map[String, String]] = kryo

    //读取nginx log
    val logs = spark.read.textFile(logPath)

    //解析nginx log
    val parseLogs = logs.map(NginxManager.accessLogParse)
      .filter(_.nonEmpty)
      .map(_.get)

    //解析get／post请求
    val decodeMaps = parseLogs.map { logMap =>
      if (logMap("nginx_http_type") == "GET") {
        val url = logMap("nginx_url")
        logMap ++ HttpManager.queryParse(HttpManager.getQuery(url)) - "nginx_url"
      } else {
        val body = logMap("nginx_body")
        logMap ++ HttpManager.bodyParse(body) - "nginx_body"
      }
    }

    //规范日志，过滤无用数据
    val institutionMatchInfo = conf.getString("institution.match.info")
    val institutionMatchMap = JsonManager.jsonToMap(institutionMatchInfo).get
    val institutionBC = spark.sparkContext.broadcast(institutionMatchMap)
    val platformMatchInfo = conf.getString("platform.match.info")
    val platformMatchMap = JsonManager.jsonToMap(platformMatchInfo).get
    val platformBC = spark.sparkContext.broadcast(platformMatchMap)
    val usertypeMatchInfo = conf.getString("usertype.match.info")
    val usertypeMatchMap = JsonManager.jsonToMap(usertypeMatchInfo).get
    val usertypeBC = spark.sparkContext.broadcast(usertypeMatchMap)

    val filterMaps = decodeMaps.map(DecodeWeiduanLog.format(_, institutionBC, platformBC, usertypeBC))
      .filter { formattedMap =>
        var institutionID = INT_DEFAULT
        Try {
          institutionID = formattedMap.getOrElse("institution_id", INT_DEFAULT.toString).asInstanceOf[String].toInt
        }

        institutionID == 1 || institutionID == 2 || institutionID == 4
      }
    //将map转为json字符串写入文件
    val jsonMaps = filterMaps.map(decodeMap => JsonManager.mapToJson(decodeMap.toMap))
      .filter(_.nonEmpty)
      .map(_.get)

    jsonMaps.write
      .mode("overwrite")
      .text(decodePath)
  }
}

object DecodeWeiduanLog {

  /**
    * 针对业务日志上报规范化
    *
    * @param logMap 日志map
    * @return 规范化后的日志map
    */
  private def format(logMap: Map[String, Any],
                     institutionBC: Broadcast[Map[String, Any]],
                     platformBC: Broadcast[Map[String, Any]],
                     usertypeBC: Broadcast[Map[String, Any]]): Map[String, Any] = {
    //过滤空格,并创建可变map
    val formattedMap = mutable.HashMap[String, Any]()
    logMap.map {
      case (key, value: String) =>
        var trimValue = value.asInstanceOf[String].trim()
        if (trimValue == "") {
          trimValue = STRING_DEFAULT
        }
        formattedMap += (key -> trimValue)
      case keyValue =>
        formattedMap += keyValue
    }

    //规范用户id
    var uid = formattedMap.get("uid") match {
      case Some(id) if id.toString != STRING_DEFAULT =>
        id.toString
      case _ =>
        var imei = STRING_DEFAULT
        Try {
          imei = formattedMap("guid").toString
        }

        imei
    }
    if (uid.length > 100) { //限制uid长度
      uid = uid.substring(0, 100)
    }
    formattedMap.update("uid", uid)

    //规范机构id
    val institutionMatchMap = institutionBC.value
    val projectID = formattedMap.getOrElse("project_id", STRING_DEFAULT).asInstanceOf[String]
    val institutionID = institutionMatchMap.getOrElse(projectID, INT_DEFAULT.toString).asInstanceOf[String]
    formattedMap.update("institution_id", institutionID)

    //规范平台类型
    val platformMatchMap = platformBC.value
    val cmdID = formattedMap.getOrElse("cmd_id", STRING_DEFAULT) match {
      case cmdStr: String =>
        cmdStr
      case cmdInt: Int =>
        cmdInt.toString
    }
    formattedMap.update("cmd_id", cmdID)
    val platform = platformMatchMap.getOrElse(cmdID, INT_DEFAULT.toString)
    formattedMap.update("platform", platform)

    //规范播放类型
    var playType = formattedMap.getOrElse("videotype", STRING_DEFAULT) match {
      case "live" | "zhibo" =>
        "1"
      case "video" | "dianbo" | "vod" =>
        "2"
      case _ =>
        INT_DEFAULT.toString
    }
    //播放类型补充
    val action = formattedMap.getOrElse("action", STRING_DEFAULT)
    if (action == "startpublish" || action == "stoppublish" || action == "uploadspeed" ||
      institutionID == "1") {
      playType = "1"
    }
    formattedMap.update("play_type", playType)

    //规范stream_id
    val streamID = formattedMap.getOrElse("streamid", STRING_DEFAULT) match {
      case "-" if formattedMap.get("url").nonEmpty =>
        var sid = "-"
        Try {
          val decodeURL = URLDecoder.decode(formattedMap("url").asInstanceOf[String], "UTF8")
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
    formattedMap.update("stream_id", streamID)

    //规范role
    val usertypeMatchMap = usertypeBC.value
    val realRole = formattedMap.getOrElse("role", STRING_DEFAULT) match {
      case role@("t" | "a" | "s") =>
        role
      case _ =>
        val usertypeKey = formattedMap.getOrElse("project_id", STRING_DEFAULT).asInstanceOf[String] +
          formattedMap.getOrElse("cmd_id", STRING_DEFAULT).asInstanceOf[String]
        usertypeMatchMap.getOrElse(usertypeKey, STRING_DEFAULT)
    }
    formattedMap.update("role", realRole)

    formattedMap.toMap
  }
}

}
