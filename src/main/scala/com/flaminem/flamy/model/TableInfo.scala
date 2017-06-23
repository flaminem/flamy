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

import com.flaminem.flamy.model.exceptions.{MergeException, FlamyException}
import com.flaminem.flamy.model.files.TableFile
import com.flaminem.flamy.model.names.{SchemaName, TableName}
import com.flaminem.flamy.model.partitions.Partition
import com.flaminem.flamy.parsing.model.TableDependency
import com.flaminem.flamy.utils.MergeableNamed
import com.flaminem.flamy.utils.logging.Logging

import scala.collection.JavaConversions._

/**
  * A TableInfo holds all the information about a table after parsing.
  * It is immutable.
  */
class TableInfo (
  val tableType: TableType,
  val tableName: TableName,
  val columns: Seq[Column] = Nil,
  val partitions: Seq[PartitionKey] = Nil,
  /**
    * The set of all tables that this table depends on.
    */
  val tableDeps: Set[TableInfo] = Set(),
  val populateInfos: Seq[PopulateInfo] = Nil
) extends TTable with MergeableNamed[TableInfo] with Logging {

  /**
    * Returns the table full name: "schema.table".
    */
  def fullName: String = {
    tableName.fullName
  }

  def schemaName: SchemaName = tableName.schemaName

  def this(table: org.apache.hadoop.hive.metastore.api.Table) {
    this(
      TableType.REMOTE,
      TableName(table.getDbName, table.getTableName),
      table.getSd.getCols.map{new Column(_)},
      table.getPartitionKeys.map{fieldSchema => new PartitionKey(new Column(fieldSchema))}
    )
  }

  override def toString: String = {
    if (columns.isEmpty && partitions.isEmpty && tableDeps.isEmpty) {
      fullName
    }
    else {
      val columnsString: String = if (this.columns.isEmpty) "" else s", columns${columns.mkString("[",", ","]")}"
      val partitionsString: String = if (this.partitions.isEmpty) "" else s", partitions${partitions.mkString("[",", ","]")}"
      val tableDepsString: String = if (this.tableDeps.isEmpty) "" else s", tableDeps${tableDeps.mkString("[",", ","]")}"
      s"TableInfo(type=$tableType, name=$tableName" + columnsString + partitionsString + tableDepsString + ")"
    }
  }

  /**
    * Merge the TableInfo with another TableInfo.
    * Columns and Partitions from the other TableInfo are kept (unless null or empty)
    * The verification that the columns and partition number are matching must be done before (elsewhere)
    *
    * @param that
    * @throws MergeException
    **/
  @throws(classOf[MergeException])
  def merge(that: TableInfo): TableInfo = {
    if (this.tableName != that.tableName) {
      throw new MergeException(s"Cannot merge tables with different names : ${this.fullName} and ${that.fullName}")
    }
    if(this.partitions.nonEmpty && that.partitions.nonEmpty && this.partitionNames!=that.partitionNames){
      throw new MergeException(
        s"Table partitions definition are differing: " +
          s"${this.partitionNames.mkString("[",", ","]")} is not equal to ${that.partitionNames.mkString("[",", ","]")}")
    }
    new TableInfo(
      this.tableType,
      this.tableName,
      this.columns,
      this.partitions,
      this.tableDeps.union(that.tableDeps),
      (this.populateInfos ++ that.populateInfos).distinct
    )
  }
}

object TableInfo {

  implicit val ord: Ordering[TableInfo] = new Ordering[TableInfo] {
    override def compare(x: TableInfo, y: TableInfo): Int = {
      x.fullName.compareToIgnoreCase(y.fullName)
    }
  }

  private[model] def getTableDependencies(table: Table): Set[TableInfo] = {
    table match {
      case td: TableDependency =>
        val tableDeps: Traversable[TableInfo] =
          for {
            t: TableDependency <- td.tableDeps
          } yield {
            assert(t.tableDeps.isEmpty)
            if(t.schemaName.isEmpty) {
              throw new FlamyException(s"Table reference not found : ${t.fullName} (please use full table names for external references)")
            }
            new TableInfo(t.tableType, TableName(t.fullName))
          }
        tableDeps.toSet
      case _ => Set()
    }
  }

  def apply(td: Table, populateInfo: PopulateInfo): TableInfo = {
    assert(td.schemaName.isDefined, s"Please report a bug: the following table should have a schemaName: $td")
    val tableDeps: Set[TableInfo] = getTableDependencies(td)
    new TableInfo(td.tableType, TableName(td.schemaName.get, td.tableName), td.columns, td.partitionKeys, tableDeps, Seq(populateInfo))
  }

  def apply(td: TableDependency, tableFile: TableFile): TableInfo = {
    assert(td.schemaName.isDefined, s"Please report a bug: the following table should have a schemaName: $td")
    val vars = new Variables()
    val tableDeps: Set[TableInfo] = getTableDependencies(td)
    val partitionConstraints = new Partition(td.partitions)::Nil
    val populateInfo = PopulateInfo(td, tableFile, vars, partitionConstraints, Nil)
    new TableInfo(td.tableType, TableName(td.schemaName.get, td.tableName), td.columns, td.partitionKeys, tableDeps, Seq(populateInfo))
  }

  def apply(td: Table): TableInfo = {
    assert(td.schemaName.isDefined, s"Please report a bug: the following table should have a schemaName: $td")
    val tableDeps = getTableDependencies(td)
    new TableInfo(td.tableType, TableName(td.schemaName.get, td.tableName), td.columns, td.partitionKeys, tableDeps, Nil)
  }

}