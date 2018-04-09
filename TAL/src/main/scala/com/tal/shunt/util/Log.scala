package com.tal.shunt.util

import org.slf4j.{Logger, LoggerFactory}

/**
  * 日志组件,通过继承该类直接调用log相关函数
  * Created by andone1cc on 2018/3/13.
  */
trait Log {
  @transient
  private[this] lazy val _log: Logger = create()

  /**
    * 创建logger对象
    * 使用该方法主要是为了在多台机器上序列化传输对象时，
    * 应该使用目标机器的配置文件，所以需要重新创建对象。
    *
    * @return 可用的logger对象
    */
  private def create(): Logger = {
    LoggerFactory.getLogger(logName)
  }

  /**
    * 获取当前类路径
    *
    * @return 返回当前类路径（过滤$符号）
    */
  private def logName: String = {
    this.getClass.getName.stripSuffix("$")
  }

  /**
    * 打印Info级别日志
    *
    * @param msg 返回需要打印string的无参函数
    */
  protected final def logInfo(msg: => String): Unit = {
    if (_log.isInfoEnabled) {
      _log.info(msg)
    }
  }

  /**
    * 打印Debug级别日志
    *
    * @param msg 返回需要打印string的无参函数
    */
  protected final def logDebug(msg: => String): Unit = {
    if (_log.isDebugEnabled) {
      _log.debug(msg)
    }
  }

  /**
    * 打印Trace级别日志
    *
    * @param msg 返回需要打印string的无参函数
    */
  protected final def logTrace(msg: => String): Unit = {
    if (_log.isTraceEnabled) {
      _log.trace(msg)
    }
  }

  /**
    * 打印Warn级别日志
    *
    * @param msg 返回需要打印string的无参函数
    */
  protected final def logWarn(msg: => String): Unit = {
    if (_log.isWarnEnabled) {
      _log.warn(msg)
    }
  }

  /**
    * 打印Error级别日志
    *
    * @param msg 返回需要打印string的无参函数
    */
  protected final def logError(msg: => String): Unit = {
    if (_log.isErrorEnabled) {
      _log.error(msg)
    }
  }

  /**
    * 打印Info级别日志，包括异常
    *
    * @param msg       返回需要打印string的无参函数
    * @param throwable 需要打印的异常
    */
  protected final def logInfo(msg: => String, throwable: Throwable): Unit = {
    if (_log.isInfoEnabled) {
      _log.info(msg, throwable)
    }
  }

  /**
    * 打印Info级别日志，包括异常
    *
    * @param msg       返回需要打印string的无参函数
    * @param throwable 需要打印的异常
    */
  protected final def logDebug(msg: => String, throwable: Throwable): Unit = {
    if (_log.isDebugEnabled) {
      _log.debug(msg, throwable)
    }
  }

  /**
    * 打印Trace级别日志，包括异常
    *
    * @param msg       返回需要打印string的无参函数
    * @param throwable 需要打印的异常
    */
  protected final def logTrace(msg: => String, throwable: Throwable): Unit = {
    if (_log.isTraceEnabled) {
      _log.trace(msg, throwable)
    }
  }

  /**
    * 打印Warn级别日志，包括异常
    *
    * @param msg       返回需要打印string的无参函数
    * @param throwable 需要打印的异常
    */
  protected final def logWarn(msg: => String, throwable: Throwable): Unit = {
    if (_log.isWarnEnabled) {
      _log.warn(msg, throwable)
    }
  }

  /**
    * 打印Error级别日志，包括异常
    *
    * @param msg       返回需要打印string的无参函数
    * @param throwable 需要打印的异常
    */
  protected final def logError(msg: => String, throwable: Throwable): Unit = {
    if (_log.isErrorEnabled) {
      _log.error(msg, throwable)
    }
  }
}
