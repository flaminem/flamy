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

package com.flaminem.flamy.parsing.hive

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model.Variables
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.utils.logging.Logging

import scala.util.control.NonFatal

/**
  * Created by fpin on 11/19/16.
  */
trait Parser[T] extends Logging {

  protected def unsafeParseQuery(query: String)(implicit context: FlamyContext): Seq[T]

  def parseQuery(query: String)(implicit context: FlamyContext): Seq[T] = {
    try {
      unsafeParseQuery(query)
    } catch {
      case e: FlamyParsingException if e.query.isDefined => throw e
      case NonFatal(e) => throw FlamyParsingException(query, e, verbose = true)
    }
  }

  protected def ignoreQuery(query: String): Boolean = {
    if(QueryUtils.isCommand(query)){
      logger.debug("Ignore query: $query")
      true
    }
    else {
      false
    }
  }

  @throws(classOf[FlamyException])
  def parseText(text: String, vars: Variables, isView: Boolean)(implicit context: FlamyContext): Seq[T] = {
    val cleanedQueries = QueryUtils.cleanAndSplitQuery(text)
    if(isView && cleanedQueries.size > 1){
      throw new FlamyParsingException("Only one query is allowed inside a view definition.")
    }
    cleanedQueries.filterNot{ignoreQuery}.flatMap{
      case query =>
        try {
          val replacedQuery = vars.replaceInText(query)
          parseQuery(replacedQuery)
        } catch {
          case e: FlamyParsingException if e.query.isDefined => throw e
          case NonFatal(e) => throw FlamyParsingException(query, e, verbose = true)
        }
    }
  }

}

