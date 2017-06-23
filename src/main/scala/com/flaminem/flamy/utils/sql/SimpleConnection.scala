/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.flaminem.flamy.utils.sql

import java.sql.{Connection, DriverManager, Statement}

import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.utils.sql.hive.CachedResultSet

import scala.util.control.NonFatal

/**
  * Created by fpin on 5/10/17.
  */
class SimpleConnection(
  val jdbcUri: String,
  val jdbcUser: String,
  val jdbcPassword: String
) extends AutoCloseable {

  import SimpleConnection._

  private val con: Connection =
    try {
      Class.forName(connectionType.driverClass)
      DriverManager.getConnection(jdbcUri, jdbcUser, jdbcPassword)
    }
    catch {
      case e : java.lang.ClassNotFoundException if connectionType == ConnectionType.MySQL =>
        throw new FlamyException(
          "Due to an incompatibility between the GPLv2 and the Apache 2.0 licenses, " +
          "this project cannot directly depend on the MySQL nor MariaDB JDBC connectors.\n" +
          "We invite you to download the MySQL jdbc connector jar and " +
          "add it manually to this project's lib/ directory."
        )
      case e : org.postgresql.util.PSQLException =>
        throw new FlamyException(s"JDBC Connection Error: ${e.getMessage}",e)
    }

  private val stmt: Statement = {
    con.createStatement()
  }

  private def connectionType = {
    ConnectionType.fromJdbcUri(jdbcUri)
  }

  private def replaceBigints: String = {
    connectionType match {
      case ConnectionType.MySQL => "SIGNED"
      case _ => "BIGINT"
    }
  }

  private def translate(s: String): String = {
    connectionType match {
      case ConnectionType.MySQL => mysqlTranslate(s)
      case _ => s
    }
  }

  override def close(): Unit = {
    stmt.close()
    con.close()
  }

  def executeQuery(query: String): CachedResultSet = {
    val translatedQuery = translate(query)
    try {
      stmt.executeQuery(translatedQuery).cache
    }
    catch {
      case NonFatal(e) => throw new FlamyException(s"Failed to execute the following query:\n$translatedQuery", e)
    }
  }

}

object SimpleConnection {


  private def mysqlTranslate(s: String): String = {
    s.replace("\"", "").replace("BIGINT", "SIGNED")
  }

}

