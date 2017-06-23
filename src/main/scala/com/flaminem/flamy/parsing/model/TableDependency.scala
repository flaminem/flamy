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

import com.flaminem.flamy.model._
import com.flaminem.flamy.model.exceptions.{MergeException, FlamyException}
import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.utils.MergeableNamed
import com.flaminem.flamy.utils.collection.immutable.NamedCollection
import com.flaminem.flamy.utils.logging.Logging

import scala.language.implicitConversions

/*
 * a TableDependency holds all the structural information of a table, and its dependency information.
 * Its fields are mutable, and it should be used only during parsing.
 */
class TableDependency(
  tableType: TableType,
  _tableName: String,
  _schemaName: Option[String]
)
extends Table(
  tableType,
  _tableName,
  _schemaName
) with MergeableNamed[TableDependency] with Logging with DependencyNode {

  /**
    * The set of all tables that this table depends on.
    */
  var tableDeps: TableDependencyCollection = new TableDependencyCollection

  /**
    * The set of all columns that this table depends on.
    */
  var colDeps: NamedCollection[ColumnDependency] = NamedCollection[ColumnDependency]()

  /**
    * The set of all columns that this table depends on after the SELECT.
    * For instance, ORDER BY or DISTRIBUTE BY clauses checks the column names after the SELECT clause.
    */
  var postColDeps: NamedCollection[ColumnDependency] = NamedCollection[ColumnDependency]()

  /**
    * The set of all columns that this table depends on, before or after the SELECT.
    * For instance, HAVING clauses checks the column names both before and after the SELECT clause.
    */
  var bothColDeps: NamedCollection[ColumnDependency] = NamedCollection[ColumnDependency]()

  /**
    * True if this table has table dependencies whose definition could not be found.
    * In such cases, column checking will not raise errors since unknown columns could come from those unknown table.
    *
    * @return
    */
  @transient
  private[model] var hasExternalTableDeps: Boolean = false

  private[model] var alreadyResolved: Boolean = false

  @throws(classOf[FlamyException])
  @transient
  lazy val hasDynamicPartitions: Boolean = {
    this.partitions.count {
      case p: PartitionColumn => p.value.isEmpty
      case p => throw new FlamyException("Bug: this should not happen")
    } match {
      case count if count == this.partitions.size => true
      case count if count == 0 => false
      case _ => throw new FlamyException(s"The partitions values of the table ${this.fullName} must either all be specified or all be dynamic.")
    }
  }

  def this(tableType: TableType, tableName: String) {
    this(tableType, tableName, None)
  }

  def this(tableType: TableType, tableName: String, schemaName: String) {
    this(tableType, tableName, Some(schemaName))
  }

  def this(tableType: TableType, fullName: TableName) {
    this(tableType, fullName.name, Some(fullName.schemaName.name))
  }

  def this(table: Table) {
    this(table.getType, table.tableName, table.schemaName)
    this.columns = table.columns
    this.partitions = table.partitions
  }

  def this(table: TableInfo) {
    this(table.getType, table.tableName.name, table.tableName.schemaName)
    this.columns = table.columns
    this.partitions = table.partitions
  }

  override def toString: String = {
    if (columns.isEmpty && partitions.isEmpty && tableDeps.isEmpty && colDeps.isEmpty) {
      fullName
    }
    else {
      val schema: String = if (this.schemaName.isEmpty) "" else s", schema=${schemaName.get}"
      val columnsString: String = if (this.columns.isEmpty) "" else s", columns${columns.mkString("[",", ","]")}"
      val partitionsString: String = if (this.partitions.isEmpty) "" else s", partitions${partitions.mkString("[",", ","]")}"
      val tableDepsString: String = if (this.tableDeps.isEmpty) "" else s", tableDeps$tableDeps"
      val colDepsString: String = if (this.colDeps.isEmpty) "" else s", colDeps$colDeps"
      val postColDepsString: String = if (this.postColDeps.isEmpty) "" else s", postColDeps$postColDeps"
      val bothColDepsString: String = if (this.bothColDeps.isEmpty) "" else s", bothColDeps$bothColDeps"
      s"TableDependency(type=$tableType, name=$tableName" + schema + columnsString + partitionsString + tableDepsString + colDepsString + postColDepsString + bothColDepsString + ")"
    }
  }

  /**
   * Merge the TableDependency with another TableDependency.
   * Columns and Partitions from the other TableDependency are kept (unless null or empty)
   * The verification that the columns and partition number are matching must be done before (elsewhere)
   *
   * @param that
   * @throws MergeException
   **/
  @throws(classOf[MergeException])
  def merge(that: TableDependency): TableDependency = {
    if (this.tableName!=that.tableName || this.schemaName!=that.schemaName) {
      throw new MergeException(s"Cannot merge tables with different names : ${this.fullName} and ${that.fullName}")
    }
    assert(that.columns!=null)
    assert(that.partitions!=null)
    if(that.columns.nonEmpty) {
      this.columns = that.columns
    }
    if(that.partitions.nonEmpty) {
      if(this.partitions.nonEmpty && this.partitionNames!=that.partitionNames){
        throw new MergeException(
          s"Table partitions definition are differing: " +
          s"${this.partitionNames.mkString("[",", ","]")} is not equal to ${that.partitionNames.mkString("[",", ","]")}")
      }
      this.partitions = that.partitions
    }
    this.tableDeps.addAll(that.tableDeps)
    this.colDeps ++= that.colDeps
    this.hasExternalTableDeps &= that.hasExternalTableDeps
    this
  }

}


object TableDependency {

  implicit def tableDependencyToTableDependencyAnalyzer(td: TableDependency): TableDependencyAnalyzer = {
    new TableDependencyAnalyzer(td)
  }

  implicit def tableDependencyToTableDependencyChecker(td: TableDependency): TableDependencyChecker = {
    new TableDependencyChecker(td)
  }

  def apply(tableType: TableType, tableName: String, schemaName: Option[String]): TableDependency = {
    TableDependency(tableType, tableName.toLowerCase, schemaName.map{_.toLowerCase})
  }


}