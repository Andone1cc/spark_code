package com.tal.shunt.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.alibaba.fastjson.serializer.SerializeFilter

import scala.collection.JavaConversions.{mapAsJavaMap, mapAsScalaMap}
import scala.util.Try
import scala.collection.Map

/**
  * @note JSON组件
  * @author andone1cc 2018/03/13
  */
object JsonManager extends Log {

  /**
    * 将map转为json
    *
    * @param map 输入格式 mutable.Map[String,Object]
    * @return
    **/
  def mapToJson(map: Map[String, Any]): Option[String] = {
    var json: Option[String] = None
    Try {
      json = Some(JSON.toJSONString(mapAsJavaMap(map), new Array[SerializeFilter](0)))
    } recover {
      case ex: Throwable =>
        logWarn("mapToJson error", ex)
    }

    json
  }

  /**
    * 将json转化为Map
    *
    * @param json 输入json字符串
    * @return 用Option包裹的结构化的scala map,如果有异常返回None
    **/
  def jsonToMap(json: String): Option[Map[String, Any]] = {
    var jsonMap: Option[Map[String, Any]] = None
    Try {
      jsonMap = Some(transformJson(JSON.parseObject(json)).asInstanceOf[Map[String, Any]])
    }

    jsonMap
  }

  /**
    * 将json转化为Array
    *
    * @param json 输入json字符串
    * @return 用Option包裹的结构化的scala array,如果有异常返回None
    **/
  def jsonToList(json: String): Option[List[Any]] = {
    var jsonList: Option[List[Any]] = None
    Try {
      jsonList = Some(transformJson(JSON.parseArray(json)).asInstanceOf[List[Any]])
    }

    jsonList
  }

  /**
    * 解析JSONObject
    *
    * @param json 源数据
    * @return map
    */
  private def transformJson(json: Any): Any = {
    json match {
      case obj: JSONObject =>
        val jsonMap = obj.getInnerMap.toMap
        jsonMap.map { case (key, value) =>
          (key, transformJson(value))
        }
      case arr: JSONArray =>
        val jsonList = arr.toArray.toList
        jsonList.map(transformJson(_))
      case other =>
        other
    }
  }

}

