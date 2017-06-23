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

/**
 * Created by fpin on 8/1/15.
 */
class Table (
  override val tableType: TableType,
  _tableName: String,
  _schemaName: Option[String],
  var columns: Seq[Column] = Nil,
  var partitions: Seq[PartitionColumn] = Nil
)
extends TTable {

  val tableName: String = _tableName.toLowerCase
  val schemaName: Option[String] = _schemaName.map{_.toLowerCase}

  def this(tableType: TableType, tableName: String) {
    this(
      tableType,
      tableName,
      None
    )
  }

  def this(tableType: TableType, tableName: String, schemaName: String) {
    this(
      tableType,
      tableName,
      Some(schemaName)
    )
  }

  def this(tableType: TableType, tableName: String, schemaName: String, columns:Seq[Column]) {
    this(
      tableType,
      tableName,
      Some(schemaName),
      columns
    )
  }

  /**
   * Returns the table full name: "schema.table".
   */
  def fullName: String = {
    schemaName match {
      case Some(schema) => s"$schema.$tableName"
      case None => tableName
    }
  }

  def getSchemaName: String = schemaName.orNull

  override def toString: String = {
    if (columns.isEmpty) {
      fullName
    }
    else {
      val schema: String = if (schemaName.isEmpty) "" else s", schema=${schemaName.get}"
      val columnsString: String = if (columns.isEmpty) "" else s", columns${columns.mkString("[",", ","]")}"
      val partitionsString: String = if (partitions.isEmpty) "" else s", partitions${partitions.mkString("[",", ","]")}"
      s"Table(type=$tableType, name=$tableName" + schema + columnsString + partitionsString + ")"
    }

  }

}
