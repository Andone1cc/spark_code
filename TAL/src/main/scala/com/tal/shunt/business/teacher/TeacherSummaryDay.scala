package com.tal.shunt.business.teacher

import com.tal.shunt.business.Schema._
import com.tal.shunt.frame.SparkFrame
import com.tal.shunt.util._
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders.kryo
import org.apache.spark.sql.functions._

/**
  * 教师数据表,天表汇总表数据入库
  * Created by andone1cc on 2018/4/3.
  */
class TeacherSummaryDay(sourcePath: String, currentDate: String, jdbcPool: JDBCManager) extends SparkFrame {

  override protected def handle(): Unit = {
    import spark.implicits._
    implicit val mapStringAnyEncoder: Encoder[Map[String, Any]] = kryo

    //获取机构信息数据
    val institutionInfoList = jdbcPool.executeQueryToProduct[InstitutionInfoDimRow](INSTITUTIONINFODIM_SELECT_SQL)
      .map(institutionInfo => (institutionInfo.id, institutionInfo))
    val institutionBC = spark.sparkContext.broadcast(institutionInfoList.toMap)

    //教师汇总数据
    val oldTeacherDetailSummaryDS = jdbcPool.executeQueryToProduct[TeacherDetailSummaryRow](TEACHERDETAILSUMMARY_SELECT_SQL).toDS

    //课程汇总数据
    val lessonDetailSummaryDS = jdbcPool.executeQueryToProduct[LessonDetailSummaryRow](LESSONDETAILSUMMARY_SELECT_SQL).toDS

  }
}
