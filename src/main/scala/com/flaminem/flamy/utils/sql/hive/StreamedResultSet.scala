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

package com.flaminem.flamy.utils.sql.hive

import com.flaminem.flamy.utils.prettyprint.Tabulator
import com.flaminem.flamy.utils.sql.{MetaData, ResultRow}

/**
  * Like a java.sql.ResultSet, but traversable. Like an Iterator, it can only be traversed once.
  * @param metaData
  * @param results
  */
class StreamedResultSet(val metaData: MetaData, val results: Iterator[ResultRow]) extends ResultSet with Iterator[ResultRow] {

  def format(): String = Tabulator.format(Seq(metaData.map{_.columnName})++results)

  override def hasNext: Boolean = results.hasNext
  override def next: ResultRow = results.next()

}


