package com.tal.shunt.business

import java.sql.Timestamp

/**
 * @note 默认值
 * @author andone1cc 2018/03/13
 */
object Default {

  val TIMESTAMP_VALUE_DEFAULT: Long = 1000L
  val TIMESTAMP_DEFAULT: Timestamp = new Timestamp(1000L)
  val TIMESTAMP_MIN: Timestamp = new Timestamp(0L)
  val TIMESTAMP_MAX: Timestamp = new Timestamp(2147483647000L)

  val INT_DEFAULT: Int = -1

  val STRING_DEFAULT: String = "-"
}