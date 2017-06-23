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

package com.flaminem.flamy.model.collection.immutable

import com.flaminem.flamy.model.TableInfo
import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.utils.collection.immutable.MergeableIndexedCollection

import scala.collection.immutable.TreeMap

/**
  * Created by fpin on 7/14/16.
  */
class MergeableTableInfoCollection private[immutable](
  override protected val map: TreeMap[TableName, TableInfo] = TreeMap[TableName, TableInfo]()
) extends TableInfoCollection with MergeableIndexedCollection[TableName, TableInfo, MergeableTableInfoCollection] {

  override def copy(map: TreeMap[TableName, TableInfo]): MergeableTableInfoCollection = {
    new MergeableTableInfoCollection(map)
  }

  def toTableInfoCollection: TableInfoCollection = {
    new TableInfoCollection(map)
  }

}

object MergeableTableInfoCollection {

  def apply(tables: TableInfo*): MergeableTableInfoCollection = {
    new MergeableTableInfoCollection() ++ tables
  }

}
