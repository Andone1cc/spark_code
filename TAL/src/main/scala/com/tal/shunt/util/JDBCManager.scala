package com.tal.shunt.util

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import java.util.concurrent.ConcurrentLinkedQueue

import com.jcraft.jsch.JSch

import scala.collection.mutable.ListBuffer
import scala.util.Try
import scala.reflect.runtime.universe._
import scala.reflect.runtime.universe

/**
  * @note JDBC组件,可以配置ssh
  * @author andone1cc 2018/03/20
  */
final class JDBCManager(driver: String, host: String, port: Int, user: String, password: String,
                        sshEnable: Boolean = false, sshHost: String = "", sshPort: Int = 0,
                        sshUser: String = "", sshPassword: String = "", proxyPort: Int = 0,
                        size: Int = 1)
  extends Log {

  private[this] lazy val sshSession = new JSch().getSession(sshUser, sshHost, sshPort)

  private[this] lazy val (realHost, realPort) = if (sshEnable) {
    sshSession.setPassword(sshPassword)
    val config = new Properties
    config.put("StrictHostKeyChecking", "no")
    sshSession.setConfig(config)
    sshSession.connect()
    sshSession.setPortForwardingL(proxyPort, host, port)

    ("127.0.0.1", proxyPort)
  } else {
    (host, port)
  }

  private[this] lazy val _pool: ConcurrentLinkedQueue[Connection] = create()

  private def create(): ConcurrentLinkedQueue[Connection] = {
    loadDriver()

    createPool()
  }

  /**
    * 加载数据库驱动
    */
  private def loadDriver(): Unit = {
    Try {
      Class.forName(driver)
    } recover {
      case ex: Throwable =>
        logError("forName error", ex)
    }
  }

  /**
    * 创建一个链接
    *
    * @return 一个数据库连接
    */
  private def createConnection(): Connection = {
    val conn = DriverManager.getConnection(s"jdbc:mysql://$realHost:$realPort" +
      "?characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false", user, password)
    conn.setAutoCommit(false)

    conn
  }

  /**
    * 根据size大小创建数据库连接池，并创建等量连接放入池中，并返回
    *
    * @return 数据库连接池
    */
  private def createPool(): ConcurrentLinkedQueue[Connection] = {
    val pool = new ConcurrentLinkedQueue[Connection]()
    (1 to size).foreach { _ =>
      Try {
        val conn: Connection = createConnection()

        pool.add(conn)
      } recover {
        case ex: Throwable =>
          logError("getConnection error", ex)
      }
    }

    pool
  }

  /**
    * 从池中获取一个连接，如果没有就等待
    *
    * @return 池中的一个连接
    */
  private def getConnection: Connection = {
    var conn: Connection = null

    conn = _pool.poll()
    while (conn == null || !conn.isValid(1)) {
      Try {
        conn = createConnection()
      } recover {
        case ex: Throwable =>
          logError("createConnection error", ex)
      }
    }

    conn
  }

  /**
    * 执行增删改SQL语句
    *
    * @param sql    要执行的sql语句
    * @param params sql语句对应的参数
    * @return 影响的行数
    */
  def executeUpdate(sql: String, params: Array[AnyRef] = Array()): Int = {
    var executeRowNum = -1
    if (params == null) {
      logError("params error")
    } else {
      val conn = getConnection

      Try {
        val ps = conn.prepareStatement(sql)
        for (i <- params.indices) {
          ps.setObject(i + 1, params(i))
        }

        executeRowNum = ps.executeUpdate
        conn.commit()
      } recover {
        case ex: Throwable =>
          logError("executeUpdate error", ex)
      }

      _pool.add(conn)
    }

    executeRowNum
  }

  /**
    * 执行查询SQL语句
    *
    * @param sql    要执行的sql语句
    * @param params sql语句对应的参数
    * @return 查询结果集
    */
  def executeQuery(sql: String, params: Array[AnyRef] = Array()): ResultSet = {
    var resultSet: ResultSet = null
    if (params == null) {
      logError("params error")
    } else {
      val conn = getConnection

      Try {
        val ps = conn.prepareStatement(sql)
        for (i <- params.indices) {
          ps.setObject(i + 1, params(i))
        }

        resultSet = ps.executeQuery
        conn.commit()
      } recover {
        case ex: Throwable =>
          logError("executeQuery error", ex)
      }

      _pool.add(conn)
    }

    resultSet
  }

  /**
    * 查询数据并转为case class类型
    *
    * @param sql 指定的查询语句,需要和case class数据一一对应
    * @tparam T case class类型
    * @return case class类型的list
    */
  def executeQueryToProduct[T <: Product : TypeTag](sql: String): List[T] = {
    val resultBuffer = scala.collection.mutable.ListBuffer[T]()

    val resultSet = executeQuery(sql)
    while (resultSet.next()) {
      val row = (1 to resultSet.getMetaData.getColumnCount).map(resultSet.getObject)

      val productApply = universe.runtimeMirror(getClass.getClassLoader)
        .reflectClass(universe.typeOf[T].typeSymbol.asClass)
        .reflectConstructor(universe.typeOf[T].decl(universe.termNames.CONSTRUCTOR).asMethod)

      resultBuffer += productApply(row: _*).asInstanceOf[T]
    }

    resultBuffer.toList
  }

  /**
    * 批量执行增删改SQL语句
    *
    * @param sql        要执行的sql语句
    * @param paramsList sql语句对应的参数列表
    * @return 影响的行数
    */
  def executeBatch(sql: String, paramsList: List[Product]): List[Int] = {
    var executeRowNumBuffer = ListBuffer[Int]()
    if (paramsList == null || paramsList.length <= 0) {
      logError("paramsList error")
    } else {
      var unloadList = paramsList
      while (unloadList.nonEmpty) {
        //批量更新个数限制
        val (currentList, otherList) = unloadList.splitAt(1000)
        unloadList = otherList

        val conn = getConnection
        Try {
          val ps = conn.prepareStatement(sql)
          currentList.foreach { product =>
            val params = product.productIterator.toArray
            params.indices.foreach(i => ps.setObject(i + 1, params(i)))

            ps.addBatch()
          }

          executeRowNumBuffer ++= ps.executeBatch.toList
          conn.commit()
        } recover {
          case ex: Throwable =>
            conn.rollback()
            logError("executeBatch error", ex)
        }

        _pool.add(conn)
      }
    }

    executeRowNumBuffer.toList
  }

  /**
    * 关闭连接池
    */
  def close(): Unit = {
    _pool.toArray.foreach(_.asInstanceOf[Connection].close())

    if (sshEnable) {
      sshSession.disconnect()
    }
  }
}

