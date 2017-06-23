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

package com.flaminem.flamy.parsing.model

import com.flaminem.flamy.model.TableInfo
import com.flaminem.flamy.model.collection.immutable.MergeableTableInfoCollection
import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.utils.collection.mutable.MergeableIndexedCollection

import scala.language.implicitConversions

/**
 * Created by fpin on 10/30/14.
 */
class MergeableTableDependencyCollection()
  extends MergeableIndexedCollection[TableName, TableDependency] {

  def this(tableDependencies: Seq[TableDependency]){
    this
    this ++= tableDependencies
  }

  def toTableDependencyCollection: TableDependencyCollection = {
    new TableDependencyCollection(this)
  }

  def toTableInfoCollection: MergeableTableInfoCollection = {
    MergeableTableInfoCollection(getAllValues.map{TableInfo(_)}.toSeq:_*)
  }

  override def getIndexOf(value: TableDependency): TableName = {
    value.fullName
  }

}

object MergeableTableDependencyCollection {
  implicit class MergeableTableDependencyCollectionConvertible(s: Seq[TableDependency]) {
    def toMergeableTableDependencyCollection: MergeableTableDependencyCollection = {
      new MergeableTableDependencyCollection(s)
    }
  }
}
