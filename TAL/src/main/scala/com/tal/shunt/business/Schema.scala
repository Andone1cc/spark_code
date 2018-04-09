package com.tal.shunt.business

import java.sql.{Date, Timestamp}

/**
  * @note 数据库结构类
  * @author andone1cc 2018/03/20
  */
object Schema {

  private val DATABASE_NAME = "weiduan"

  /*create table zh_lesson_info_dim(
  auto_id int(11) not null auto_increment comment '自增主键',
  id varchar(50) comment '课程id',
  stream_id varchar(50) comment '流id',
  `name` varchar(255) comment '课程名称',
  type tinyint(2) comment '课程类型',
  teach_type tinyint(2) comment '授课方式',
  begin_time timestamp comment '预约上课时间',
  end_time timestamp comment '预约下课时间',
  teacher_id varchar(50) comment '教师id',
  teacher_name varchar(50) comment '教师名称',
  institut_id int(11) comment '机构id',
  primary key(auto_id)
  )*/
  val LESSONINFODIM_SELECT_SQL: String = "select id, stream_id, `name`, type, teach_type, begin_time, end_time, " +
    s"teacher_id, teacher_name, institution_id from $DATABASE_NAME.zh_lesson_info_dim"

  val LESSONINFODIM_REPLACE_SQL: String = s"replace into $DATABASE_NAME.zh_lesson_info_dim(id, " +
    "stream_id, `name`, type, teach_type, begin_time, end_time, teacher_id, teacher_name, " +
    "institution_id) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

  case class LessonInfoDimRow(id: String, streamID: String, name: String, `type`: Int, teachType: Int,
                              beginTime: Timestamp, endTime: Timestamp, teacherID: String, teacherName: String,
                              institutionID: Int)


  /*  create table zh_institution_info_dim(
      auto_id int(11) not null auto_increment comment '自增主键',
      id int(11) comment '机构id',
      `name` varchar(50) comment '机构名称',
      type tinyint(2) comment '机构类型',
      primary key(auto_id)
    )*/
  val INSTITUTIONINFODIM_SELECT_SQL: String = s"select id, `name`, type  from $DATABASE_NAME.zh_institution_info_dim"

  case class InstitutionInfoDimRow(id: Int, name: String, `type`: Int)


  /*  create table zh_institution_detail_day(
      auto_id int(11) not null auto_increment comment '自增主键',
      id int(11) comment '机构id',
      `name` varchar(50) comment '机构名称',
      type tinyint(2) comment '机构类型',
      lesson_type tinyint(2) comment '课程类型',
      student_num int(11) comment '学生个数',
      lesson_num int(11) comment '课程个数',
      listen_time int(11) comment '听课时长',
      play_type tinyint(2) comment '播放类型',
      platform tinyint(2) comment '平台',
      dt date comment '日期',
      primary key(auto_id)
    )*/
  val INSTITUTIONDETAILDAY_REPLACE_SQL: String = s"insert into $DATABASE_NAME.zh_institution_detail_day(id, " +
    "`name`, type, lesson_type, student_num, lesson_num, lesson_distinct_num, listen_time, play_type, platform, dt) " +
    "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) on duplicate key update `name` = values(name), type = values(type), " +
    "lesson_type = values(lesson_type), student_num = values(student_num), lesson_num = values(lesson_num), " +
    "lesson_distinct_num = values(lesson_distinct_num), listen_time = values(listen_time)"

  case class InstitutionDetailDayRow(id: Int, name: String, `type`: Int, lessonType: Int, studentNum: Long,
                                     lessonNum: Long, lessonDistinctNum: Long, listenTime: Long, playType: Int,
                                     platform: Int, dt: Date)

  case class InstitutionSourceRow(id: Int, platform: Int, playType: Int, action: String, role: String, userID: String,
                                  streamID: String, name: String, `type`: Int, lessonType: Int)


  /*  create table zh_lesson_detail_summary(
      auto_id int(11) not null auto_increment comment '自增主键',
      id varchar(50) comment '课程id',
      stream_id varchar(50) comment '流id',
      `name` varchar(255) comment '课程名称',
      type tinyint(2) comment '课程类型',
      teach_type tinyint(2) comment '授课方式',
      teacher_id varchar(50) comment '教师id',
      teacher_name varchar(50) comment '教师名称',
      begin_time timestamp comment '预约上课时间',
      end_time timestamp comment '预约下课时间',
      real_begin_time timestamp comment '实际上课时间',
      real_end_time timestamp comment '实际下课时间',
      `status` tinyint(2) comment '课堂状态',
      institution_id int(11) comment '机构id',
      today_student_num int(11) comment '当日学生数',
      total_student_num int(11) comment '总学生数',
      today_listen_time int(11) comment '当日时长',
      total_listen_time int(11) comment '总时长',
      today_lesson_num int(11) comment '当日次数',
      total_lesson_num int(11) comment '总次数',
      play_type tinyint(2) comment '播放类型',
      platform tinyint(2) comment '平台',
      primary key(auto_id)
    ))*/
  val LESSONDETAILSUMMARY_SELECT_SQL: String = "select id, stream_id, `name`, type, teach_type, teacher_id, " +
    "teacher_name, begin_time, end_time, real_begin_time, real_end_time, `status`, institution_id, today_student_num, " +
    "total_student_num, today_listen_time, total_listen_time, today_lesson_num, total_lesson_num, play_type, " +
    s"platform from $DATABASE_NAME.zh_lesson_detail_summary"

  val LESSONDETAILSUMMARY_REPLACE_SQL: String = s"insert into $DATABASE_NAME.zh_lesson_detail_summary(id, " +
    "stream_id, name, `type`, teach_type, teacher_id, teacher_name, begin_time, end_time, real_begin_time, " +
    "real_end_time, status, institution_id, today_student_num, total_student_num, today_listen_time, total_listen_time, " +
    "today_lesson_num, total_lesson_num, play_type, platform) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
    "?, ?, ?, ?, ?, ?, ?, ?, ?) on duplicate key update id = values(id), `name` = values(`name`), type = values(type), " +
    "teach_type = values(teach_type), teacher_id = values(teacher_id), teacher_name = values(teacher_name), " +
    "begin_time = values(begin_time), end_time = values(end_time), real_begin_time = values(real_begin_time), " +
    "real_end_time = values(real_end_time), `status` = values(`status`), today_student_num = values(today_student_num), " +
    "total_student_num = values(total_student_num), today_listen_time = values(today_listen_time), " +
    "total_listen_time = values(total_listen_time), today_lesson_num = values(today_lesson_num), " +
    "total_lesson_num = values(total_lesson_num)"

  val LESSONDETAILSUMMARY_UPDATE_SQL: String = s"update $DATABASE_NAME.zh_lesson_detail_summary set `status` = 4 " +
    "where `status` = 1 and to_days(begin_time) < to_days(currentDate) and unix_timestamp(begin_time) != 1"

  case class LessonDetailSummaryRow(id: String, streamID: String, name: String, `type`: Int, teachType: Int,
                                    teacherID: String, teacherName: String, beginTime: Timestamp, endTime: Timestamp,
                                    realBeginTime: Timestamp, realEndTime: Timestamp, status: Int, institutionID: Int,
                                    todayStudentNum: Int, totalStudentNum: Int, todayListenTime: Int,
                                    totalListenTime: Int, todayLessonNum: Int, totalLessonNum: Int, playType: Int,
                                    platform: Int)

  case class LessonSourceRow(institutionID: Int, platform: Int, playType: Int, action: String, role: String,
                             userID: String, streamID: String, time: Timestamp)


  /*  create table zh_student_lesson_detail_summary(
        auto_id int(11) not null auto_increment comment '自增主键',
        institution_id int(11) comment '机构id',
        lesson_id varchar(50) comment '课程id',
        stream_id varchar(100) comment '流id',
        lesson_name varchar(255) comment '课程名称',
        lesson_type tinyint(2) comment '课程类型',
        teach_type tinyint(2) comment '授课方式',
        begin_time timestamp comment '预约上课时间',
        end_time timestamp comment '预约下课时间',
        `status` tinyint(2) comment '课堂状态',
        student_id varchar(50) comment '学生id',
        student_name varchar(50) comment '学生姓名',
        teacher_id varchar(50) comment '老师id',
        teacher_name varchar(50) comment '老师姓名',
        first_enter_time timestamp comment '初次进入时间',
        last_exit_time timestamp comment '最后离开时间',
        enter_num int(11) comment '进入次数',
        listen_time int(11) comment '听课时长',
        location varchar(50) comment '位置信息',
        platform tinyint(2) comment '平台',
        primary key(auto_id)
    );*/
  val STUDENTLESSONDETAILSUMMARY_SELECT_SQL: String = "select institution_id, lesson_id, stream_id, lesson_name, " +
    "lesson_type, teach_type, begin_time, end_time, `status`, student_id, student_name, teacher_id, teacher_name, " +
    "first_enter_time, last_exit_time, enter_num, listen_time, location, platform " +
    s"from $DATABASE_NAME.zh_student_lesson_detail_summary"

  val STUDENTLESSONDETAILSUMMARY_REPLACE_SQL: String = s"insert into $DATABASE_NAME.zh_student_lesson_detail_summary(" +
    "institution_id, lesson_id, stream_id, lesson_name, lesson_type, teach_type, begin_time, end_time, `status`, " +
    "student_id, student_name, teacher_id, teacher_name, first_enter_time, last_exit_time, enter_num, listen_time, " +
    "location, platform) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) on duplicate key update " +
    "lesson_id = values(lesson_id), lesson_name = values(lesson_name), lesson_type = values(lesson_type), " +
    "teach_type = values(teach_type), begin_time = values(begin_time), end_time = values(end_time), `status` = values(`status`), " +
    "student_name = values(student_name), teacher_id = values(teacher_id), teacher_name = values(teacher_name), " +
    "first_enter_time = ifnull(first_enter_time, values(first_enter_time)), " +
    "last_exit_time = values(last_exit_time), enter_num = enter_num + values(enter_num), " +
    "listen_time = listen_time + values(listen_time), location = values(location)"

  case class StudentLessonDetailSummaryRow(institutionID: Int, lessonID: String, streamID: String, lessonName: String,
                                           lessonType: Int, teachType: Int, beginTime: Timestamp, endTime: Timestamp,
                                           status: Int, studentID: String, studentName: String, teacherID: String,
                                           teacherName: String, firstEnterTime: Timestamp, lastExitTime: Timestamp,
                                           enterNum: Long, listenTime: Long, location: String, platform: Int)

  case class LessonStudentSourceRow(institutionID: Int, platform: Int, playType: Int, action: String, role: String,
                                    userID: String, streamID: String, time: Timestamp, ip: String)


  /*  create table zh_student_detail_summary(
        auto_id int(11) not null auto_increment comment '自增主键',
        id varchar(50) comment '学生id',
        `name` varchar(50) comment '学生姓名',
        institution_id int(11) comment '机构id',
        institution_name varchar(50) comment '机构名称',
        sex tinyint(2) comment '学生性别',
        location varchar(50) comment '位置信息',
        phone varchar(50) comment '手机号',
        grade varchar(50) comment '年级',
        register_date timestamp comment '创建时间',
        today_lesson_num int(11) comment '当日观看次数',
        total_lesson_num int(11) comment '总观看次数',
        today_lesson_distinct_num int(11) comment '当日去重观看次数',
        total_lesson_distinct_num int(11) comment '累加去重观看次数',
        today_listen_time int(11) comment '当日观看时长',
        total_listen_time int(11) comment '总观看时长',
        play_type tinyint(2) comment '播放类型',
        platform tinyint(2) comment '平台 1pc 2phone 3web',
        primary key(auto_id)
    )
    create table zh_student_action_day(
      auto_id int(11) not null auto_increment comment '自增主键',
      id varchar(50) comment '学生id',
      `name` varchar(50) comment '学生姓名',
      institution_id int(11) comment '机构id',
      lesson_num int(11) comment '上课次数次数',
      lesson_distinct_num int(11) comment '去重课程数',
      listen_time int(11) comment '观看时长',
      play_type tinyint(2) comment '播放类型',
      platform varchar(50) comment '平台',
      dt date comment '日期',
      primary key(auto_id)
    );
    */
  val STUDENTDETAILSUMMARY_SELECT_SQL: String = "select id, `name`, institution_id, institution_name, sex, location, " +
    "phone, grade, register_date, today_lesson_num, total_lesson_num, today_lesson_distinct_num, total_lesson_distinct_num, " +
    s"today_listen_time, total_listen_time, play_type, platform from $DATABASE_NAME.zh_student_detail_summary"

  val STUDENTACTIONDAY_REPLACE_SQL: String = s"insert into $DATABASE_NAME.zh_student_action_day(id, `name`, " +
    "institution_id, lesson_num, lesson_distinct_num, listen_time, play_type, platform, dt) values(?, ?, ?, ?, ?, " +
    "?, ?, ?, ?) on duplicate key update `name` = values(`name`), lesson_num = values(lesson_num), " +
    "lesson_distinct_num = values(lesson_distinct_num), listen_time = values(listen_time)"

  val STUDENTDETAILSUMMARY_REPLACE_SQL: String = s"insert into $DATABASE_NAME.zh_student_detail_summary(id, " +
    "`name`, institution_id, institution_name, sex, location, phone, grade, register_date, today_lesson_num, total_lesson_num," +
    "today_lesson_distinct_num, total_lesson_distinct_num, today_listen_time, total_listen_time, play_type, platform) " +
    "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
    "on duplicate key update `name` = values(`name`), institution_name = values(institution_name), sex = values(sex), " +
    "location = values(location), phone = values(phone), grade = values(grade), register_date = values(register_date), " +
    "today_lesson_num = values(today_lesson_num), " +
    "total_lesson_num = total_lesson_num + values(total_lesson_num), " +
    "today_lesson_distinct_num = values(today_lesson_distinct_num), " +
    "total_lesson_distinct_num = total_lesson_distinct_num + values(total_lesson_distinct_num), " +
    "today_listen_time = values(today_listen_time), " +
    "total_listen_time = total_listen_time + values(total_listen_time)"

  case class StudentActionDayRow(studentID: String, studentName: String, institutionID: Int, lessonNum: Long,
                                 lessonDistinctNum: Long, listenTime: Long, playType: Int, platform: Int, dt: Date)

  case class StudentDetailSummaryRow(studentID: String, studentName: String, institutionID: Int, institutionName: String,
                                     sex: Int, location: String, phone: String, grade: String, registerDate: Timestamp,
                                     todayLessonNum: Long, totalLessonNum: Long, todayLessonDistinctNum: Long,
                                     totalLessonDistinctNum: Long, todayListenTime: Long, totalListenTime: Long,
                                     playType: Int, platform: Int)

  case class StudentSummaryDaySourceRow(institutionID: Int, institutionName: String, platform: Int, playType: Int,
                                        action: String, role: String, userID: String, streamID: String, time: Timestamp,
                                        ip: String)


  /*  #教师汇总数据
    create table zh_teacher_detail_summary(
      auto_id int(11) not null auto_increment comment '自增主键',
      id varchar(50) comment '教师id',
      name varchar(50) comment '教师名称',
      institution_id int(11) comment '机构id',
      institution_name varchar(50) comment '机构名称',
      sex tinyint(2) comment '教师性别',
      location varchar(50) comment '位置信息',
      phone varchar(50) comment '手机号',
      register_date timestamp comment '创建时间',
      today_lesson_num int(11) comment '当日上课次数',
      total_lesson_num int(11) comment '总上课次数',
      today_lesson_distinct_num int(11) comment '当日去重观看次数',
      total_lesson_distinct_num int(11) comment '累加去重观看次数',
      today_lesson_time int(11) comment '当日上课时长',
      total_lesson_time int(11) comment '总上课时长',
      today_student_num int(11) comment '当日学生数',
      total_student_num int(11) comment '总学生数',
      today_study_play_num int(11) comment '当日被学生点播次数',
      total_study_play_num int(11) comment '总被学生点播次数',
      today_study_play_time int(11) comment '当日被学生点播时长',
      total_study_play_time int(11) comment '总被学生点播时长',
      today_study_live_num int(11) comment '当日被学生直播次数',
      total_study_live_num int(11) comment '总被学生直播次数',
      today_study_live_time int(11) comment '当日被学生直播时长',
      total_study_live_time int(11) comment '总被学生直播时长',
      primary key(auto_id)
    );

    #教师每日数据
    create table zh_teacher_action_day(
      auto_id int(11) not null auto_increment comment '自增主键',
      id varchar(50) comment '教师id',
      name varchar(50) comment '教师名称',
      institution_id int(11) comment '机构id',
      lesson_num int(11) comment '上课次数',
      lesson_distinct_num int(11) comment '去重课程数',
      lesson_time int(11) comment '上课时长',
      student_num int(11) comment '学生数',
      study_play_num int(11) comment '被学生点播次数',
      study_play_time int(11) comment '被学生点播时长',
      study_live_num int(11) comment '被学生直播次数',
      study_live_time int(11) comment '当日被学生直播时长',
      dt date comment '日期',
      primary key(auto_id)
    );*/
  val TEACHERDETAILSUMMARY_SELECT_SQL: String = "select id, `name`, institution_id, institution_name, sex, location, " +
    "phone, register_date, today_lesson_num, total_lesson_num, today_lesson_distinct_num, total_lesson_distinct_num, " +
    "today_lesson_time, total_lesson_time, today_student_num, total_student_num, today_study_play_num, " +
    "total_study_play_num, today_study_play_time, total_study_play_time, today_study_live_num, total_study_live_num, " +
    s"today_study_live_time, total_study_live_time from $DATABASE_NAME.zh_teacher_detail_summary"

  val TEACHERACTIONDAY_REPLACE_SQL: String = s"insert into $DATABASE_NAME.zh_teacher_action_day(id, `name`, " +
    "institution_id, lesson_num, lesson_distinct_num, lesson_time, student_num, study_play_num, study_play_time, " +
    "study_live_num, study_live_time, dt) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) on duplicate key update " +
    "`name` = values(`name`), lesson_num = values(lesson_num), lesson_distinct_num = values(lesson_distinct_num), " +
    "lesson_time = values(lesson_time), student_num = values(student_num), study_play_num = values(study_play_num), " +
    "study_play_time = values(study_play_time), study_live_num = values(study_live_num), study_live_time = values(study_live_time)"

  val TEACHERDETAILSUMMARY_REPLACE_SQL: String = s"insert into $DATABASE_NAME.zh_teacher_detail_summary(id, `name`, " +
    "institution_id, institution_name, sex, location, phone, register_date, today_lesson_num, total_lesson_num, " +
    "today_lesson_distinct_num, total_lesson_distinct_num, today_lesson_time, total_lesson_time, today_student_num, " +
    "total_student_num, today_study_play_num, " +
    "total_study_play_num, today_study_play_time, total_study_play_time, today_study_live_num, total_study_live_num, " +
    "today_study_live_time, total_study_live_time) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
    "?, ?, ?, ?, ?, ?, ?) on duplicate key update `name` = values(`name`), institution_name = values(institution_name), " +
    "sex = values(sex), location = values(location), phone = values(phone), register_date = values(register_date), " +
    "today_lesson_num = values(today_lesson_num), total_lesson_num = values(total_lesson_num), " +
    "today_lesson_distinct_num = values(today_lesson_distinct_num), total_lesson_distinct_num = values(total_lesson_distinct_num), " +
    "today_lesson_time = values(today_lesson_time), total_lesson_time = values(total_lesson_time), " +
    "today_student_num = values(today_student_num), total_student_num = values(total_student_num), " +
    "today_study_play_num = values(today_study_play_num), total_study_play_num = values(total_study_play_num), " +
    "today_study_play_time = values(today_study_play_time), total_study_play_time = values(total_study_play_time), " +
    "today_study_live_num = values(today_study_live_num), total_study_live_num = values(total_study_live_num), " +
    "today_study_live_time = values(today_study_live_time), total_study_live_time = values(total_study_live_time)"

  case class TeacherActionDayRow(teacherID: String, teacherName: String, institutionID: Int, lessonNum: Long,
                                 lessonDistinctNum: Long, lessonTime: Long, studentNum: Long, studyPlayNum: Long,
                                 studyPlayTime: Long, studyLiveNum: Long, studyLiveTime: Long, dt: Date)

  case class TeacherDetailSummaryRow(teacherID: String, teacherName: String, institutionID: Int, institutionName: String,
                                     sex: Int, location: String, phone: String, registerDate: Timestamp,
                                     todayLessonNum: Long, totalLessonNum: Long, todayLessonDistinctNum: Long,
                                     totalLessonDistinctNum: Long, todayLessonTime: Long, totalLessonTime: Long,
                                     todayStudentNum: Long, totalStudentNum: Long, todayStudyPlayNum: Long,
                                     totalStudyPlayNum: Long, todayStudyPlayTime: Long, totalStudyPlayTime: Long,
                                     todayStudyLiveNum: Long, totalStudyLiveNum: Long, todayStudyLiveTime: Long,
                                     totalStudyLiveTime: Long)

  case class TeacherSummaryDaySourceRow(institutionID: Int, institutionName: String, action: String, role: String,
                                        userID: String, streamID: String, time: Timestamp, ip: String)

}
