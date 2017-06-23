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

import com.flaminem.flamy.model._
import com.flaminem.flamy.model.names.{ItemName, TableName}
import com.flaminem.flamy.parsing.ParsingUtils
import com.flaminem.flamy.parsing.model.MergeableTableDependencyCollection._
import com.flaminem.flamy.parsing.model.{MergeableTableDependencyCollection, TableDependency, TableDependencyCollection}
import com.flaminem.flamy.utils.logging.Logging

import scala.util.matching.Regex
import scala.util.matching.Regex.Match

/**
 * This parser is used to perform a preliminary parsing that will only fetch dependencies between tables and ignore variables.
 * Created by fpin on 11/20/14.
 */
object PopulatePreParser extends Logging{

  import com.flaminem.flamy.parsing.ParsingUtils._


  private val insertPattern = s"INSERT${s}(?:OVERWRITE${s}|INTO${s})?TABLE"
  private val createViewPattern = s"CREATE${s}VIEW(?:${s}IF${s}NOT${s}EXISTS)?"
  private val loadDataPattern = s"LOAD${s}DATA${s}(?:LOCAL${s})?INPATH${s}[^${s}]+${s}OVERWRITE${s}INTO${s}TABLE"

  /* (?i) at the beginning of a Regex means case insensitive.
   * In order to try keeping things readable, we keep spaces and replace them */
  private val firstCTE_RE = new Regex(s"(?i)\\A${z}WITH($simpleTableNamePattern)${z}AS${z}[(]")
  private val nextCTE_RE = new Regex(s"(?i)$z,$z($noSpaceSimpleTableNamePattern)${z}AS$z[(]")

  private val TABLE_NAME = "tableName"
  private val PAR = "par"

  private val destRE = new Regex(s"(?i)(?:$insertPattern|$loadDataPattern)($tableNamePattern)$z", TABLE_NAME)
  private val viewRE = new Regex(s"(?i)(?:$createViewPattern)($tableNamePattern)$z", TABLE_NAME)

  private val sourceRE = new Regex(s"(?i)(?:FROM|JOIN)(?:$z([(])|($tableNamePattern)$z)", PAR, TABLE_NAME)
  private val nextSourceRE =
    new Regex(s"(?i)^$z(?:AS$tableNamePattern|$noSpaceTableNamePattern)?$z,$z(?:([(])|($noSpaceTableNamePattern)$z)", PAR, TABLE_NAME)
  private[hive] val afterParenthesisSourceRE =
    new Regex(s"(?i)^$z(?:AS$simpleTableNamePattern|$noSpaceSimpleTableNamePattern)$z,$z(?:([(])|($noSpaceTableNamePattern)$z)", PAR, TABLE_NAME)

  /**
   * Find all table names corresponding to a given regexp.
   * Used to find source tables, destination tables or views depending on the regexp
   * @param re
   * @param query
   * @return
   */
  private def getTableNamesFromRegex(re: Regex, query: String): Seq[String] = {
    re
    .findAllMatchIn(query)
    .toSeq
    .map{_.group(TABLE_NAME).replaceAll("`","").toLowerCase.trim}
    .distinct
  }

  private def nameToTableDep(name: String, tableType: TableType, ctes: Set[String]) = {
    ItemName.parse(name) match {
      case Some(t: TableName) => Some(new TableDependency(tableType, t))
      case _ if ctes.contains(name.toLowerCase) => None
      case _ =>
        throw new FlamyParsingException(
          s"Wrong table name ($name). Please use a fully qualified table name, or if this name refers to a CTE make sure it correct."
        )
    }
  }

  /**
   * Replace every strings in the query by empty strings.
   * String content is useless for the parsing but may be harmful when containing matchable keywords.
   * @param query
   */
  private[hive] def emptyStrings(query: String): String = {
    def replacer(m: Regex.Match): String = m.group(1) + m.group(1)
    ParsingUtils.quotedStringRegex.replaceAllIn(query,replacer _)
  }


  private def findNextSourceTable(q: String, index: Int, regex: Regex): Option[Match] = {
    logger.debug(s"findNextSourceTable: ${q.substring(index)}")
    regex.findFirstMatchIn(q.substring(index))
  }

  private def recFindSourceTables(query: String, index: Int, regex: Regex, next: Boolean): List[String] = {
    findNextSourceTable(query, index, regex) match {
      case None if next => recFindSourceTables(query, index, sourceRE, next = false)
      case None => Nil
      case Some(m) =>
        if(m.group(PAR) == "("){
          val afterParenthesis = ParsingUtils.skipParentheses(query, index + m.end(1) - 1)
          recFindSourceTables(query, afterParenthesis, afterParenthesisSourceRE, next = true) ++
          recFindSourceTables(query.substring(index + m.end(1), afterParenthesis-1), 0, nextSourceRE, next = true)
        }
        else{
          val t = m.group(TABLE_NAME).replaceAll("`","").toLowerCase.trim
          logger.debug(s"match found: $t")
          t::recFindSourceTables(query, index + m.end(2), nextSourceRE, next = true)
        }
    }
  }

  private[hive] def findSourceTables(query: String) = {
    recFindSourceTables(query, 0, sourceRE, next = false)
  }

  private def findNextCTE(q: String, index: Int, regex: Regex): Option[Regex.Match] = {
    logger.debug(s"findNextCTE: ${q.substring(index)}")
    regex.findAllMatchIn(q.substring(index)).toSeq match {
      case matches if regex==firstCTE_RE && matches.size > 1 =>
        throw new FlamyParsingException("Wrong CTE syntax (WITH ... AS): only one WITH keyword is accepted, multiple CTEs must be comma separated")
      case matches if matches.nonEmpty =>

        Some(matches.head)
      case _ =>
        None
    }
  }

  private def recFindCTEs(query: String, index: Int, regex: Regex): List[String] = {
    findNextCTE(query, index, regex) match {
      case None => Nil
      case Some(m) =>
        val t = m.group(1).replaceAll("`","").toLowerCase.trim
        logger.debug(s"match found: $t")
        t::recFindCTEs(query, ParsingUtils.skipParentheses(query, index + m.end(1)), nextCTE_RE)
    }
  }

  private[hive] def findCTEs(query: String) = {
    recFindCTEs(query, 0, firstCTE_RE)
  }

  private def parseQuery(query: String): MergeableTableDependencyCollection = {
    val cteTables: Set[String] = findCTEs(query).map{_.toLowerCase}.toSet
    logger.debug(s"CTE founds: $cteTables")
    val destTables =
      getTableNamesFromRegex(destRE,query).flatMap{ nameToTableDep(_,TableType.TABLE,Set()) }++
      getTableNamesFromRegex(viewRE,query).flatMap{ nameToTableDep(_,TableType.VIEW,Set()) }
    val srcTables = findSourceTables(query).flatMap{ nameToTableDep(_,TableType.REF, cteTables) }
    destTables.foreach{ t => t.tableDeps = new TableDependencyCollection(srcTables)}
    destTables.toMergeableTableDependencyCollection
  }

  def parseText(text: String): MergeableTableDependencyCollection = {
    val queries: Seq[String] = QueryUtils.cleanAndSplitQuery(text).map{emptyStrings}
    queries.flatMap{parseQuery}.toMergeableTableDependencyCollection
  }

}
