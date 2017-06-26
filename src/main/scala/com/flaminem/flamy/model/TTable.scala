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

import com.flaminem.flamy.utils.Named

/**
  * This trait put in common methods
  * for the different types of representation of tables.
  */
trait TTable extends Named {

  def fullName: String

  def tableType: TableType

  def columns: Seq[Column]

  def partitions: Seq[PartitionColumn]

  def getName: String = fullName



  def columnNames: Seq[String] = columns.map{_.columnName}

  def partitionNames: Seq[String] = partitions.map{_.columnName}

  def partitionKeys: Seq[PartitionKey] = partitions.map{new PartitionKey(_)}

  def columnsAndPartitions: Seq[Column] = columns++partitions

  def columnAndPartitionNames: Seq[String] = columnsAndPartitions.map{_.columnName}

  def getType: TableType = tableType

  def isTable: Boolean = {
    tableType == TableType.TABLE
  }

  def isTemp: Boolean = {
    tableType == TableType.TEMP
  }

  def isRef: Boolean = {
    tableType == TableType.REF
  }

  def isView: Boolean = {
    tableType == TableType.VIEW
  }

  def isExt: Boolean = {
    tableType == TableType.EXT
  }

  def isPartitioned: Boolean = {
    partitionKeys.nonEmpty
  }

  override def equals(other: Any): Boolean = {
    other match {
      case that: TTable =>
        this.fullName.equalsIgnoreCase(that.fullName)
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    this.fullName.hashCode
  }

}

object TTable {

  implicit val ord: Ordering[TTable] = new Ordering[TTable] {
    override def compare(x: TTable, y: TTable): Int = {
      x.fullName.compareToIgnoreCase(y.fullName)
    }
  }

}