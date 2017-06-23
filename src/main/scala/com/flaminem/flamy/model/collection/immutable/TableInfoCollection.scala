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
import com.flaminem.flamy.utils.collection.immutable.IndexedCollection

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ListBuffer
import scala.collection.{TraversableLike, mutable}

/**
  * Created by fpin on 7/13/16.
  */
class TableInfoCollection private[immutable](
  override protected val map: TreeMap[TableName, TableInfo] = TreeMap[TableName, TableInfo]()
) extends IndexedCollection[TableName, TableInfo, TableInfoCollection](map)
  with TraversableLike[TableInfo, TableInfoCollection]{

  def getTables: Iterable[TableInfo] = {
    super.getAllValues
  }

  override def getIndexOf(value: TableInfo): TableName = {
    value.fullName
  }

  override def copy(map: TreeMap[TableName, TableInfo]): TableInfoCollection = {
    new TableInfoCollection(map)
  }

  override def newBuilder: mutable.Builder[TableInfo, TableInfoCollection] = {
    new ListBuffer[TableInfo].mapResult{x => TableInfoCollection(x:_*)}
  }

}

object TableInfoCollection {

  def apply(tables: TableInfo*): TableInfoCollection = {
    new TableInfoCollection() ++ tables
  }



}
