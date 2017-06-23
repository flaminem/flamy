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

import java.util

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model.{IOFormat, PartitionColumn, PartitionKey}
import org.apache.hadoop.hive.metastore.api.StorageDescriptor

import scala.collection.JavaConversions._

/**
  * A Partition's metadata build from Hive's thrift client information.
  */
class PartitionWithEagerInfo (
  override val creationTime: Option[Long],
  override val location: String,
  val ioFormat: IOFormat,
  override val columns: Seq[PartitionColumn],
  fileSize: Option[Long] = None,
  fileCount: Option[Long] = None,
  val modificationTime: Option[Long] = None
) extends Partition(columns) with PartitionWithInfo {

  override def getFileSize: Option[Long] = {
    fileSize
  }

  override def getFileCount: Option[Long] = {
    fileCount
  }

  override def getModificationTime(context: FlamyContext, refresh: Boolean = false): Option[Long] = {
    modificationTime
  }

}

object PartitionWithEagerInfo {

  private def apply(createTime: Int, sd: StorageDescriptor, parameters: Map[String, String], columns: Seq[PartitionColumn]): PartitionWithEagerInfo = {
    new PartitionWithEagerInfo(
      Option(createTime).map{_.toLong * 1000},
      sd.getLocation,
      IOFormat(sd),
      columns,
      parameters.get("totalSize").map{_.toLong},
      parameters.get("numFiles").map{_.toLong},
      parameters.get("transient_lastDdlTime").map{_.toLong * 1000}
    )
  }

  def apply(p: org.apache.hadoop.hive.metastore.api.Partition, partitionKeys: Seq[PartitionKey]): PartitionWithEagerInfo = {
    val partitionValues = p.getValues.toList
    val columns: Seq[PartitionColumn] = partitionKeys.zip(partitionValues).map{case (k,v) => new PartitionColumn(k, v)}
    PartitionWithEagerInfo(p.getCreateTime, p.getSd, p.getParameters.toMap, columns)
  }

  def apply(t: org.apache.hadoop.hive.metastore.api.Table): PartitionWithEagerInfo = {
    PartitionWithEagerInfo(t.getCreateTime, t.getSd, t.getParameters.toMap, Nil)
  }


}