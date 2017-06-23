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

import com.flaminem.flamy.model.files.TableFile
import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.model.partitions.Partition
import com.flaminem.flamy.model.partitions.transformation.{Annotation, Ignore, IgnoreTimestamp}
import com.flaminem.flamy.parsing.model.TableDependency

/**
  * Created by fpin on 7/13/16.
  */
case class PopulateInfo (
  tableFile: TableFile,
  tableDependencies: Set[TableName],
  hasDynamicPartitions: Boolean,
  variables: Variables,
  partitionConstraints: Seq[Partition],
  annotations: Seq[Annotation]
) {

  def title: String = {
    s"${tableFile.tableFileName} : $variables"
  }

  val constantPartitions: Seq[Partition] = {
    partitionConstraints.map{p => p.filter{_.hasConstantValue}}
  }

  def tableName: TableName = tableFile.tableName

  /**
    * Compute the set of partition variables that are used in this populate
    *
    * @return
    */
  lazy val usedPartitionVariables: Set[String] = {
    Variables.findInText(tableFile.text)
      .filter{_.startsWith("partition:")}
      .map{_.replaceFirst("partition:", "")}
      .toSet
  }

  def hasPartitionVariables: Boolean = {
    usedPartitionVariables.nonEmpty
  }

  /* This is legacy for for old_regen */
  def ignoredTables: Set[TableName] = {
    annotations.view
    .collect{case t : Ignore => t}
    .map{_.table}
    .toSet
  }

  /* This is for regen2 */
  def tablesWithIgnoreTimestamp: Set[TableName] = {
    annotations.view
      .collect{case t : IgnoreTimestamp => t}
      .map{_.table}
      .toSet
  }

}

object PopulateInfo {

  def apply(
    tableDependency: TableDependency,
    tableFile: TableFile,
    variables: Variables,
    partitionConstraints: Seq[Partition],
    partitionTransformations: Seq[Annotation]
  ): PopulateInfo = {
    new PopulateInfo(
      tableFile,
      tableDependency.tableDeps.map{td => TableName(td.fullName)}.toSet,
      tableDependency.hasDynamicPartitions,
      variables,
      partitionConstraints,
      partitionTransformations
    )
  }


}