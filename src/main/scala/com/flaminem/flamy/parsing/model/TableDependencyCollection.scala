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

import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.utils.collection.mutable.IndexedCollection

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer
import scala.collection.{TraversableLike, mutable}

/**
 * Created by fpin on 10/30/14.
 */
class TableDependencyCollection
extends IndexedCollection[TableName, TableDependency]
with TraversableLike[TableDependency, TableDependencyCollection]{

  override def newBuilder: mutable.Builder[TableDependency, TableDependencyCollection] = {
    TableDependencyCollection.canBuildFrom()
  }

  def this(tables: Traversable[TableDependency]) = {
    this
    this++=tables
  }

  /**
    * Return a fresh copy of this collection
    */
  def copy(): TableDependencyCollection = {
    val res = new TableDependencyCollection
    res ++= this
    res
  }

  def getTables: Iterable[TableDependency] = {
    super.getAllValues
  }

  override def getIndexOf(value: TableDependency): TableName = {
    value.fullName
  }

}

object TableDependencyCollection {

  implicit val canBuildFrom = new CanBuildFrom[TableDependencyCollection, TableDependency, TableDependencyCollection] {
    override def apply(from: TableDependencyCollection): mutable.Builder[TableDependency, TableDependencyCollection] = {
      apply()
    }
    override def apply(): mutable.Builder[TableDependency, TableDependencyCollection] = {
      new ListBuffer[TableDependency] mapResult{x => new TableDependencyCollection(x)}
    }
  }

  implicit class TableDependencyCollectionConvertible(s: Seq[TableDependency]) {
    def toTableDependencyCollection: TableDependencyCollection = {
      new TableDependencyCollection(s)
    }
  }

}
