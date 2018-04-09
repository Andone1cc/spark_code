package com.tal.shunt.extra


import scala.language.implicitConversions

/** 原有公共类加强包
  * Created by andone1cc on 2018/3/13.
  */
object Extra {

  // 隐式地转换类型
  implicit def toStringExtra(str: String): StringExtra = {
    new StringExtra(str)
  }

}
