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

package com.flaminem.flamy.model.partitions

import com.flaminem.flamy.model.PartitionColumn

/**
  * Created by fpin on 7/26/16.
  */
case class PartitionColumnFilter private[partitions] (
  partitionColumn: PartitionColumn,
  operator: RangeOperator
) {

  def apply(pc: PartitionColumn): Boolean = {
    operator(pc, partitionColumn)
  }

  override def toString: String = {
    partitionColumn.columnName + operator.name + partitionColumn.value
  }

}


object PartitionColumnFilter {

  /**
    * Builds a PartitionColumnFilter from a string.
    * Returns None is the string format is not correct.
    * @param string
    * @return
    */
  def fromString(string: String): Option[PartitionColumnFilter] = {
    val operatorsPattern = RangeOperator.values.map{_.name}.mkString("(","|",")")
    val regex = s"([^<>=]*?)$operatorsPattern([^<>=]*)".r
    string match {
      case regex(key, op, value) =>
        val partitionColumn = new PartitionColumn(key.toLowerCase, value)
        Some(new PartitionColumnFilter(partitionColumn, RangeOperator(op)))
      case _ =>
        None
    }
  }

}

