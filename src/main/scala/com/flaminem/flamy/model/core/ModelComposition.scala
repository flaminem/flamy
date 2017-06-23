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

package com.flaminem.flamy.model.core

import com.flaminem.flamy.model._
import com.flaminem.flamy.model.collection.immutable.TableInfoCollection
import com.flaminem.flamy.model.files.{FileIndex, FileType}
import com.flaminem.flamy.model.names.{ItemName, TableName}

/**
  * Extending this trait allows a Model to have generic chainable operations that preserve the original types, such as filters.
  * The abstract {{copy}} method must be implemented.
  */
trait ModelComposition[+Repr] {
  self: Repr =>

  val tables: TableInfoCollection
  val fileIndex: FileIndex

  /**
    * Return a copy of this object, with the specified fields changed
    *
    * @param tables
    * @param fileIndex
    */
  def copy(
    tables: TableInfoCollection = tables,
    fileIndex: FileIndex = fileIndex
  ): Repr


  def filter(predicate: (ItemName)=>Boolean): Repr = {
    val filteredTableInfoSet: TableInfoCollection = tables.filter{td => predicate(TableName(td.fullName))}
    val filteredFileIndex = fileIndex.filter{predicate}
    copy(tables=filteredTableInfoSet, fileIndex=filteredFileIndex)
  }

  /**
    * Return a copy of this where only the filetypes satisfying the given predicate are kept.
 *
    * @param predicate
    * @return
    */
  def filterFileTypes(predicate: (ItemName, FileType) => Boolean): Repr = {
    copy(tables, fileIndex.filterFileTypes(predicate))
  }

  def filter(items: Traversable[ItemName], acceptIfEmpty: Boolean=true): Repr = {
    if(acceptIfEmpty && items.isEmpty){
      self
    }
    else {
      val itemFilter = new ItemFilter(items,acceptIfEmpty)
      filter(itemFilter)
    }
  }

}

