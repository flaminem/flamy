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

package com.flaminem.flamy.model.metadata

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model.IOFormat
import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.model.partitions.TablePartitioningInfo
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


/**
  * A Table's metadata build from Hive's thrift client information.
  */
class TableWithInfo (
  override val creationTime: Option[Long],
  override val location: String,
  val ioFormat: IOFormat,
  val name: TableName,
  val numPartitions: Option[Int],
  fileSize: Option[Long],
  fileCount: Option[Long],
  modificationTime: Option[Long]
) extends ItemWithInfo {

  override def toString: String = {
    name.toString
  }

  def hasNoInfo: Boolean = {
    TableWithInfo.formatWithoutInfo.contains("ioFormat.toString")
  }

  override def getFileSize: Option[Long] = {
    if(hasNoInfo) {
      None
    }
    else {
      fileSize
    }
  }

  override def getFileCount: Option[Long] = {
    if(hasNoInfo) {
      None
    }
    else {
      fileCount
    }
  }

  override def getModificationTime(context: FlamyContext, refresh: Boolean = false): Option[Long] = {
    modificationTime
  }

  def formattedNumPartitions: String = {
    numPartitions.map{_.toString}.getOrElse("")
  }

  override def getFormattedInfo(context: FlamyContext, humanReadable: Boolean): Seq[String] = {
    Seq(
      name.toString,
      formattedNumPartitions,
      formattedFileSize(context, humanReadable),
      formattedFileCount(context),
      formattedModificationTime(context),
      ioFormat.toString
    )
  }

}

object TableWithInfo {

  private final val formatWithoutInfo = Set("VIEW", "HBASE")

  val getSparkSchema: StructType = {
    StructType(Seq(
      StructField("table", StringType),
      StructField("num_partitions", LongType),
      StructField("size", LongType),
      StructField("num_files", LongType),
      StructField("modification_time", LongType),
      StructField("format", StringType)
    ))
  }

  val getInfoHeader: Seq[String] = {
    getSparkSchema.fields.map{_.name}
  }

  def apply(table: org.apache.hadoop.hive.metastore.api.Table): TableWithInfo = {
    val sd = table.getSd
    val ioFormat = IOFormat(sd.getInputFormat,sd.getOutputFormat,sd.getSerdeInfo.getSerializationLib)
    val parameters = table.getParameters
    new TableWithInfo(
      Option(table.getCreateTime),
      sd.getLocation,
      ioFormat,
      TableName(table.getDbName, table.getTableName),
      None,
      Option(parameters.get("totalSize")).map{_.toLong}.orElse{Some(0L)},
      Option(parameters.get("numFiles")).map{_.toLong}.orElse{Some(0L)},
      Option(parameters.get("transient_lastDdlTime")).map{_.toLong*1000}
    )
  }

  def apply(table: org.apache.hadoop.hive.metastore.api.Table, tpInfo: TablePartitioningInfo): TableWithInfo = {
    val sd = table.getSd
    val ioFormat = IOFormat(sd.getInputFormat,sd.getOutputFormat,sd.getSerdeInfo.getSerializationLib)
    val modifTimes: Seq[Long] = tpInfo.tablePartitions.flatMap{_.getModificationTime(null)}
    val modifTime: Option[Long] =
      if(modifTimes.isEmpty) {
        Option(table.getParameters.get("transient_lastDdlTime")).map{_.toLong*1000}
      }
      else {
        Some(modifTimes.max)
      }
    new TableWithInfo(
      Option(table.getCreateTime),
      sd.getLocation,
      ioFormat,
      TableName(table.getDbName, table.getTableName),
      Some(tpInfo.size),
      Some(tpInfo.tablePartitions.flatMap{_.getFileSize}.sum),
      Some(tpInfo.tablePartitions.flatMap{_.getFileCount}.sum),
      modifTime
    )
  }

}
