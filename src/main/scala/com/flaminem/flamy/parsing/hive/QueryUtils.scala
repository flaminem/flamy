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


import com.flaminem.flamy.parsing.ParsingUtils

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
 * Created by fpin on 11/20/14.
 */
object QueryUtils {

  import com.flaminem.flamy.parsing.ParsingUtils._

  protected val partitionPattern: Regex = s"(?i)\\s*partitions:".r

  private val stringPattern = """(?<quote>['"]).*?(?<!\\)\k<quote>"""

  protected val annotationRE: Regex = s"""\\A(@[-_\\p{Alnum}]+)$z[(]""".r

  private final val semicolonRE: Regex = new Regex("""(?<!\\);""")
  private final val commentRE: Regex = new Regex("--[^\\n\\r]*([\\n\\r]|$)")

  /**
    * Returns true if the query is a Hive command like "SET ..." or "ADD JAR ..."
    * @param query
    * @return
    */
  def isCommand(query: String): Boolean = {
    query.toUpperCase match {
      case start if start.startsWith("SET") => true
      case start if start.startsWith("ADD JAR") => true
      case _ => false
    }
  }

  def splitQuery(query: String): Seq[String] = {
    semicolonRE
      .split(query)
      .map{_.trim}
      .filter{!_.isEmpty}
      .toSeq
  }

  /**
   * Remove line comments.
   * @param query
   * @return
   */
  def cleanQuery(query: String): String = {
    commentRE.replaceAllIn(query," ")
  }

  def removeAnnotations(query: String): String = {
    var s = query
    var continue = true
    while(continue){
      findNextCharOutsideQuotes(s.toCharArray, '@', 0) match {
        case None =>
          continue = false
        case Some(i) =>
          val before = s.substring(0, i)
          s = s.substring(i)
          annotationRE.findFirstMatchIn(s) match {
            case None => throw new FlamyParsingException("Unrecognized annotation")
            case Some(m) =>
              val after = ParsingUtils.skipParentheses(s, m.end-1) ;
              s = before + s.substring(after)
          }
      }
    }
    s
  }

  def findAnnotations(query: String): Seq[(String, String)] = {
    val buf = new ListBuffer[(String, String)]()
    var s = query
    var continue = true
    while(continue){
      findNextCharOutsideQuotes(s.toCharArray, '@', 0) match {
        case None =>
          continue = false
        case Some(i) =>
          val before = s.substring(0, i)
          s = s.substring(i)
          annotationRE.findFirstMatchIn(s) match {
            case None =>
              continue = false
            case Some(m) =>
              val after = ParsingUtils.skipParentheses(s, m.end-1) ;
              buf += m.group(1).substring(1) -> s.substring(m.end, after-1)
              s = m.before(1) + s.substring(after)
          }
      }
    }
    buf
  }

  /**
    * Remove line comments and split the queries separated by a semicolon.
    * Partition transformations are removed.
    * @param query
    * @return
    */
  def cleanAndSplitQuery(query: String): Seq[String] =  {
    splitQuery(removeAnnotations(cleanQuery(query)))
  }

}


