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

import com.flaminem.flamy.model.columns.{ColumnValue, NoValue}
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.model.names.PartitionColumnName

/**
 * A partition column (name=value), ex: day=2014-03-03
 */
class PartitionColumn(
  override val rawColumnName: String,
  _columnType: Option[String],
  override val comment: Option[String],
  override val value: ColumnValue
)
extends Column(rawColumnName, _columnType, comment, value) {

  override def copy(
    rawColumnName: String = this.rawColumnName,
    columnType: Option[String] = this.columnType,
    comment: Option[String] = this.comment,
    value: ColumnValue = this.value
  ): PartitionColumn = {
    new PartitionColumn(rawColumnName, columnType, comment, value)
  }

  def this(col: Column) {
    this(col.columnName, col.columnType, col.comment, col.value)
  }

  def this(col: Column, value: Option[String]) {
    this(col.columnName, col.columnType, col.comment, ColumnValue(value))
  }

  def this(col: Column, value: String) {
    this(col, Option(value))
  }

  def this(name: String, value: ColumnValue) {
    this(name, None, None, value)
  }

  def this(name: String, value: Option[String] = None, colType: Option[String] = None, comment: Option[String] = None) {
    this(name, colType, comment, ColumnValue(value))
  }

  def this(name: String, value: String) {
    this(name, Option(value))
  }

  // TODO: Default name should be configurable
  def partitionColumnName: PartitionColumnName = {
    val stringValue =
      value match {
        case NoValue => "_HIVE_DEFAULT_PARTITION_"
        case v => v.toString
      }
    PartitionColumnName(columnName + "=" + stringValue)
  }

  /**
    * Return the value of this partitionColumn, potentially wrapped in quotes depending of its type,
    * so that it can be used as is in a SQL query.
    * @return
    */
  def stringValue: String = {
    this.columnType match {
      case Some("tinyint")
         | Some("smallint")
         | Some("int")
         | Some("bigint") => value.toString
      case Some("string") => "\"" + value + "\""
      case Some("boolean") =>
        throw new FlamyException("Boolean partitions are broken, you should not use them (see HIVE-6590)")
      case _ =>
        throw new FlamyException(
          s"Other partition types than (tinyint, int, smallint, bigint, boolean, string) are not supported yet. " +
          s"Got $this of type $columnType"
        )
    }
  }

  override def toString: String = {
    value match {
      case NoValue => super.toString
      case v => s"$columnName=$v"
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: PartitionColumn =>
      columnName == that.columnName && value == that.value
    case _ => false
  }

  override def hashCode: Int = {
    var result: Int = if (value.isDefined) value.hashCode else 0
    result = 31 * result + columnName.hashCode
    result
  }

}
