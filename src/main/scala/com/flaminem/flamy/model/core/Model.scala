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

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.exec.files.FileRunner
import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.files.FileIndex
import com.flaminem.flamy.model.names.{ItemName, TableName}


trait Model extends ModelComposition[Model] {
  /**
    * True if this object was build with a CompleteModelFactory (rather than an IncompleteModelFactory)
    */
  val isComplete: Boolean = {
    this match {
      case _ : CompleteModel => true
      case _ => false
    }
  }

  def getTable(tableName: TableName): Option[TableInfo] = tables.get(tableName.fullName)

  def getAllTables: Iterable[TableInfo] = tables.toIterable

  def getAllTableNames: Iterable[TableName] = tables.map{t => TableName(t.fullName)}.toIterable

  override def toString: String = f"Model(${tables.toString}})"
}


object Model{

  def getIncompleteModel(context: FlamyContext, items:Iterable[ItemName]): IncompleteModel = {
    val index = context.getFileIndex.strictFilter(items).get
    new IncompleteModelFactory(context).generateModel(index)
  }

  def getIncompleteModel(context: FlamyContext, fileIndex: FileIndex): IncompleteModel = {
    new IncompleteModelFactory(context).generateModel(fileIndex)
  }

  def getCompleteModel(context: FlamyContext, items: Iterable[ItemName]): CompleteModel = {
    getCompleteModel(context, ItemList(items.toSeq))
  }

  def getCompleteModel(context: FlamyContext, itemArgs: ItemArgs, checkNoMissingTable: Boolean = false): CompleteModel = {
    val tableGraph = TableGraph(context, itemArgs, checkNoMissingTable)
    new CompleteModelFactory(context, tableGraph).generateModel()
  }

  /**
    * Performs a quick check by building a Model and return the FileRunner's stats.
    *
    * @param context
    * @param items
    * @return
    */
  /* This method is here because we want to keep the factories private */
  def quickCheck(context: FlamyContext, items:Iterable[ItemName]): Option[FileRunner#Stats] = {
    val tableGraph = TableGraph(context, items.toSeq, checkNoMissingTable = false)
    val completeModelFactory: CompleteModelFactory =  new CompleteModelFactory(context, tableGraph)
    completeModelFactory.generateModel()
    Option(completeModelFactory.getStats)
  }

}
