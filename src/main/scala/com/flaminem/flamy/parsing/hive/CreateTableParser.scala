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

import java.io.IOException

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model.Table
import org.apache.hadoop.hive.ql.parse.{ParseException, SemanticException}

/**
 * Created by fpin on 7/31/15.
 */
object CreateTableParser {

  @throws(classOf[SemanticException])
  @throws(classOf[ParseException])
  @throws(classOf[IOException])
  def parseQuery(query: String)(implicit context: FlamyContext): Table = {
    val cti: CreateTableInfo = new CreateTableInfo(context)
    cti.parse(query)
  }

  @throws(classOf[SemanticException])
  @throws(classOf[ParseException])
  @throws(classOf[IOException])
  def parseText(text: String)(implicit context: FlamyContext): Table = {
    val queries: Seq[String] = QueryUtils.cleanAndSplitQuery(text)
    if (queries.size != 1) {
      throw new RuntimeException("More than 1 query parsed")
    }
    parseQuery(queries.iterator.next)
  }
}
