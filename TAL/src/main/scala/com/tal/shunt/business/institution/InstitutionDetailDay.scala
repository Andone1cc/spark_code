package com.tal.shunt.business.institution

import com.tal.shunt.frame.SparkFrame
import com.tal.shunt.business.Default._
import com.tal.shunt.business.Schema._
import com.tal.shunt.util._
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders.kryo
import org.apache.spark.sql.functions._

import scala.collection.Map

/** @note 机构每日数据入库
  *       Created by andone1cc on 2018/3/20.
  */
class InstitutionDetailDay(sourcePath: String, currentDate: String, jdbcPool: JDBCManager) extends SparkFrame {

  override protected def handle(): Unit = {
    import spark.implicits._
    implicit val mapStringAnyEncoder: Encoder[Map[String, Any]] = kryo

    val institutionInfoList = jdbcPool.executeQueryToProduct[InstitutionInfoDimRow](INSTITUTIONINFODIM_SELECT_SQL)
      .map(institutionInfo => (institutionInfo.id, institutionInfo))
    val institutionBC = spark.sparkContext.broadcast(institutionInfoList.toMap)

    val jsonDS = spark.read.textFile(sourcePath)
    val logMapDS = jsonDS.map(JsonManager.jsonToMap(_).get)

    val sourceDS = logMapDS.map { logMap =>
      val id = logMap.getOrElse("institution_id", INT_DEFAULT.toString).asInstanceOf[String].toInt
      val platform = logMap.getOrElse("platform", INT_DEFAULT.toString).asInstanceOf[String].toInt
      val playType = logMap.getOrElse("play_type", INT_DEFAULT.toString).asInstanceOf[String].toInt
      val action = logMap.getOrElse("action", STRING_DEFAULT).asInstanceOf[String]
      val role = logMap.getOrElse("role", STRING_DEFAULT).asInstanceOf[String]
      val userID = logMap.getOrElse("uid", STRING_DEFAULT).asInstanceOf[String]
      val streamID = logMap.getOrElse("stream_id", STRING_DEFAULT).asInstanceOf[String]

      val institutionInfoMap = institutionBC.value
      val name = institutionInfoMap.get(id).map(_.name).getOrElse(STRING_DEFAULT)
      val `type` = institutionInfoMap.get(id).map(_.`type`).getOrElse(INT_DEFAULT)

      InstitutionSourceRow(id, platform, playType, action, role, userID, streamID, name, `type`, 1)
    }

    val dt = DateManager.parseSqlDate(currentDate, "yyyyMMdd").get
    val institutionDetailDS = sourceDS.groupBy('id, 'platform, 'playType)
      .agg(
        countDistinct(when(('platform === 2 || 'platform === 3) && 'userID =!= STRING_DEFAULT, 'userID)) as 'studentNum,
        count(when(('platform === 2 || 'platform === 3) && 'action === "startplay", 1)) as 'lessonNum,
        countDistinct(when(('role === "t" || 'role === "a") && 'action === "startpublish", 'streamID)) as 'lessonDistinctNum,
        sum(when(('platform === 2 || 'platform === 3) && 'action === "heartbeat", 30).otherwise(0)) as 'listenTime,
        collect_list('name)(0) as 'name,
        collect_list('type)(0) as 'type,
        collect_list('lessonType)(0) as 'lessonType,
        lit(dt) as 'dt

      ).as[InstitutionSourceRow]

    institutionDetailDS.foreachPartition { partition =>
      if (partition.nonEmpty) {
        val confFile = Thread.currentThread().getContextClassLoader.getResourceAsStream("project.properties")
        val conf = ConfManager(confFile)
        val partitionJdbcPool = new JDBCManager(conf.getString("jdbc.driver"), conf.getString("mysql.host"),
          conf.getInt("mysql.port"), conf.getString("mysql.user"), conf.getString("mysql.password"),
          conf.getBoolean("ssh.enable"), conf.getString("ssh.host"), conf.getInt("ssh.port"),
          conf.getString("ssh.user"), conf.getString("ssh.password"), HttpManager.getFreePort,
          conf.getInt("connection.num")
        )

        partitionJdbcPool.executeBatch(INSTITUTIONDETAILDAY_REPLACE_SQL, partition.toList)

        partitionJdbcPool.close()
        confFile.close()
      }
    }

  }
