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

import com.flaminem.flamy.conf.ConfigurationException

import scala.util.matching.Regex

sealed trait ConnectionType {

  val driverClass: String

}

object ConnectionType {

  val jdbcRegex: Regex = """jdbc:(\p{Alpha}+)://.*""".r

  def fromJdbcUri(jdbcUri: String): ConnectionType = {
    jdbcUri match {
      case jdbcRegex("mysql") => ConnectionType.MySQL
      case jdbcRegex("postgresql") => ConnectionType.PostgreSQL
      case _ =>
        throw new ConfigurationException(
          s"Could not parse the jdbc uri : $jdbcUri\n" +
            s"It should be of the form jdbc:<DBTYPE>://<HOST>[:<PORT>]/<SCHEMA>[?[PARAMETERS] \n" +
            s"Currently, only mysql and postgresql backends are supported"
        )
    }
  }

  object MySQL extends ConnectionType {

    override val driverClass: String = "com.mysql.jdbc.Driver"

  }

  object PostgreSQL extends ConnectionType {

    override val driverClass: String = "org.postgresql.Driver"

  }

}
