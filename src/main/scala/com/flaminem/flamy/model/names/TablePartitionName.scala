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

package com.flaminem.flamy.model.names

import com.flaminem.flamy.model.{PartitionColumn, PartitionKey}
import com.flaminem.flamy.parsing.ParsingUtils

import scala.language.{higherKinds, implicitConversions}

/**
 * The full name given to one partition of a table
 * (eg: "stats.daily_visitors/day=2014-10-12/campaign=shoes")
 * @param fullName
 */
class TablePartitionName(val fullName: String) extends ItemName {
  lazy val schemaName: SchemaName = SchemaName(tableFullName.split('.')(0))
  lazy val tableName: TableName = TableName(tableFullName)
  private lazy val tableFullName: String = fullName.split('/')(0)

  lazy val partitionName: String = partColNames.mkString("/")
  lazy val partColNames: Seq[PartitionColumnName] = fullName.split('/').tail.map{PartitionColumnName}

  def partitionKeys: Seq[PartitionKey] = partColNames.map{_.toPartitionKey}

  def partitionColumns: Seq[PartitionColumn] = partColNames.map{_.toPartitionColumn}

  def isInSchema(schema: ItemName): Boolean = schema match {
    case s: SchemaName => s.equals(schemaName)
    case default => false
  }
  def isInTable(schema: ItemName): Boolean = schema match {
    case t: TableName => t.equals(tableName)
    case default => false
  }
  override def isInOrEqual(that: ItemName): Boolean = that match {
    case name: SchemaName => this.isInSchema(name)
    case name: TableName => this.isInTable(name)
    case name: TablePartitionName => name==this
    case _ => false
  }
}

object TablePartitionName {

  def apply(fullName: String): TablePartitionName = {
    parse(fullName).getOrElse{
      throw new IllegalArgumentException(s"$fullName is not a correct TablePartitionName")
    }
  }

  def parse(s: String): Option[TablePartitionName] = {
    val t = ParsingUtils.t
    val tablePartitionRegex = s"\\A($t[.]$t)/(.*)\\z".r
    s match {
      case tablePartitionRegex(tableName, partitionName) =>
        Some(new TablePartitionName(tableName.toLowerCase + "/" + partitionName))
      case _ => None
    }
  }

  def unapply(tablePartitionName: TablePartitionName): Option[String] = Some(tablePartitionName.fullName)

  def apply(tableName: TableName, columnNames: PartitionColumnName*): TablePartitionName = {
    new TablePartitionName(tableName.fullName + "/" + columnNames.mkString("/"))
  }

  def apply(tableName: TableName, columnNames: String): TablePartitionName = {
    new TablePartitionName(tableName.fullName + "/" + columnNames)
  }

  implicit val order: Ordering[TablePartitionName] = new Ordering[TablePartitionName]{
    override def compare(x: TablePartitionName, y: TablePartitionName): Int = x.fullName.compareTo(y.fullName)
  }
}

