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

import com.flaminem.flamy.model.Table
import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.utils.collection.immutable.IndexedCollection

import scala.collection.immutable.TreeMap
import scala.collection.mutable.ListBuffer
import scala.collection.{TraversableLike, mutable}

/**
 * Created by fpin on 10/30/14.
 */
class TableCollection private (
  override protected val map: TreeMap[TableName, Table] = TreeMap[TableName, Table]()
) extends IndexedCollection[TableName, Table, TableCollection](map) with TraversableLike[Table, TableCollection]{

  def getTables: Iterable[Table] = {
    super.getAllValues
  }

  override def getIndexOf(value: Table): TableName = {
    value.fullName
  }

  override def copy(map: TreeMap[TableName, Table]): TableCollection = {
    new TableCollection(map)
  }

  override def newBuilder: mutable.Builder[Table, TableCollection] = {
    new ListBuffer[Table].mapResult{x => TableCollection(x:_*)}
  }

}

object TableCollection {

  def apply(tables: Table*): TableCollection = {
    new TableCollection() ++ tables
  }

}
