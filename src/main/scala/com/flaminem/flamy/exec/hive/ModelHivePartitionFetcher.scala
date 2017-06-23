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

package com.flaminem.flamy.exec.hive

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.names.{ItemName, TableName, TablePartitionName}
import com.flaminem.flamy.model.partitions.{PartitionWithInfo, TablePartitioningInfo}

/**
  * A simple HivePartitionFetcher that always return empty partitions.
  * Useful for mocking.
  */
class ModelHivePartitionFetcher(context: FlamyContext, items: ItemName*) extends ModelHiveTableFetcher(context, items:_*) with HivePartitionFetcher {

  override def clearCache(): Unit = {
    /* no cache used here */
  }

  override def close(): Unit = {

  }

  override def listPartitionNames(table: TableName): Iterable[TablePartitionName] = {
    Nil
  }

  override def getTablePartitioningInfo(tableName: TableName): TablePartitioningInfo = {
    val partitionKeys: Seq[PartitionKey] = getTable(tableName).get.partitionKeys
    new TablePartitioningInfo(tableName, partitionKeys, Nil)
  }

  override def getPartition(tablePartitionName: TablePartitionName): Option[PartitionWithInfo] = {
    None
  }

}
