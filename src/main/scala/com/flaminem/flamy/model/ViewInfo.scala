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

package com.flaminem.flamy.model

import com.flaminem.flamy.model.exceptions.MergeException
import com.flaminem.flamy.model.files.TableFile
import com.flaminem.flamy.model.names.TableName

/**
  * Created by fpin on 11/3/16.
  */
class ViewInfo private (
  override val tableName: TableName,
  override val columns: Seq[Column] = Nil,
  override val tableDeps: Set[TableInfo] = Set(),
  val tableFile: TableFile
)
extends TableInfo(TableType.VIEW, tableName, columns, partitions = Nil, tableDeps, populateInfos = Nil) {

  /**
    * Merge the TableInfo with another TableInfo.
    * Columns and Partitions from the other TableInfo are kept (unless null or empty)
    * The verification that the columns and partition number are matching must be done before (elsewhere)
    *
    * @param that
    * @throws MergeException
    **/
  @throws(classOf[MergeException])
  override def merge(that: TableInfo): TableInfo = {
    throw new MergeException(s"This should not happen: view ${this.tableName} is being merged with ${that.tableName}")
  }

}
object ViewInfo {

  def apply(td: Table, tableFile: TableFile): ViewInfo = {
    assert(td.tableType == TableType.VIEW, s"Please report a bug: table ($td) is not a view, it is of type ${td.getType}")
    assert(td.schemaName.isDefined, s"Please report a bug: the following table should have a schemaName: $td")
    val tableDeps = TableInfo.getTableDependencies(td)
    new ViewInfo(TableName(td.schemaName.get, td.tableName), td.columns, tableDeps, tableFile)
  }

}