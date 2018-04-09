package com.tal.shunt.extra

import org.apache.commons.lang.StringUtils

/**
  * 字符串加强组件
  * Created by andone1cc on 2018/3/13.
  */
final class StringExtra(str: String) {

  /**
    * 判断字符串是否为空
    *
    * @return 是否为空
    */
  @inline
  def isEmptyEx: Boolean = {
    str == null || str == ""
  }

  /**
    * 判断字符串是否不为空
    *
    * @return 是否不为空
    */
  @inline
  def nonEmptyEx: Boolean = {
    !isEmptyEx
  }

  /**
    * 不为空就返回自身,为空就返回default
    *
    * @param default 默认字符串
    * @return 判断后的字符串
    */
  @inline
  def nonEmptyExOrElse(default: => String): String = {
    if (isEmptyEx) {
      default
    } else {
      str
    }
  }

  /**
    * 按指定分符拆分字符串（非正则）
    *
    * @return 是否不为空
    */
  def splitEx(separator: String, num: Int = -1): Array[String] = {
    StringUtils.splitByWholeSeparator(str, separator, num)
  }

  /**
    * 查找并替换第一个字符串
    *
    * @param search      查找的字符串
    * @param replacement 替换的字符串
    * @return 替换后的结果字符串
    */
  def replaceFirstEx(search: String, replacement: String): String = {
    StringUtils.replaceOnce(str, search, replacement)
  }

  @inline
  override def toString: String = {
    str
  }
}
