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

import com.flaminem.flamy.model.Table
import com.flaminem.flamy.model.exceptions.{FlamyException, UnexpectedBehaviorException}
import com.flaminem.flamy.utils.logging.Logging

/**
  * Created by fpin on 9/12/16.
  */
class TableDependencyChecker(that: TableDependency) extends Logging {

  /**
    * Check that the number of columns correspond between the TableDependency and the table definition.
    *
    * @param tableDef
    */
  def checkColumnAndPartitions(tableDef: Table): Unit = {
    /* We need to force the evaluation of lazy val hasDynamicPartition now, before the partitions are modified */
    val hasDynamicPartitions = that.hasDynamicPartitions
    def tdColNum: Int = that.columns.size
    val tdPartNum: Int = that.partitions.size
    val tableDefPartNum: Int = tableDef.partitions.size
    val tableDefColNum: Int = tableDef.columns.size
    logger.debug(
      s"""
         |checkColumnAndPartitions: ${that.fullName}
         |tdColNum: $tdColNum
         |tdPartNum: $tdPartNum
         |tableDefPartNum: $tableDefPartNum
         |tableDefColNum: $tableDefColNum
       """.stripMargin
    )
    if (tdPartNum != tableDefPartNum && tdColNum + tdPartNum > 0) {
      throw new FlamyException(s"Table ${that.fullName} is defined with $tableDefPartNum partitions but $tdPartNum are inserted.")
    }
    if (hasDynamicPartitions && !that.hasExternalTableDeps && tdColNum > 0) {
      if(tdColNum == tableDefColNum + tableDefPartNum) {
        propagatePartitionColumnValues()
      }
      else {
        throw new FlamyException(
          s"Table ${that.fullName} has $tableDefColNum columns and $tableDefPartNum dynamic partitions, " +
            s"but the number of inserted columns is $tdColNum.\n" +
            s"When using dynamic partitioning, it is expected to be equal to the number of columns plus the number of partitions (${tableDefColNum + tableDefPartNum})."
        )
      }
    }
    if (tdColNum != tableDefColNum && !that.hasExternalTableDeps && tdColNum > 0) {
      throw new FlamyException(s"Table ${that.fullName} is defined with $tableDefColNum columns but $tdColNum are inserted.")
    }
  }

  /**
    * When dynamic partitioning is used,
    * we check if the columns corresponding to the partition have a value,
    * and if they have, we add it to the partition.
    */
  private def propagatePartitionColumnValues(): Unit = {
    val partNum = that.partitions.size
    val cols = that.columns.takeRight(partNum)
    val newPartitions =
      that.partitions.zip(cols).map{
        case (part, col) if col.value.isDefined =>
          if(part.value.isDefined && part.value != col.value) {
            throw new UnexpectedBehaviorException()
          }
          else{
            part.copy(value = col.value)
          }
        case (part, _) => part
      }
    that.columns = that.columns.dropRight(partNum)
    that.partitions = newPartitions
  }

  override def toString: String = {
    that.toString
  }

}
