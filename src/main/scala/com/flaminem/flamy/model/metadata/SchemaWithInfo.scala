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
import com.flaminem.flamy.model.metadata.TableWithInfo.getSparkSchema
import com.flaminem.flamy.model.names.{SchemaName, TableName}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
 * Created by fpin on 5/20/15.
 */
class SchemaWithInfo(
  override val creationTime: Option[Long],
  override val location: String,
  val name: SchemaName,
  val numTables: Option[Int],
  fileSize: Option[Long],
  fileCount: Option[Long],
  modificationTime: Option[Long]
) extends ItemWithInfo {

  def formattedNumTables: String = {
    numTables.map{_.toString}.getOrElse("")
  }

  override def getFormattedInfo(context: FlamyContext, humanReadable: Boolean): Seq[String] = {
    Seq(
      name.toString,
      formattedNumTables,
      formattedFileSize(context, humanReadable),
      formattedFileCount(context),
      formattedModificationTime(context)
    )
  }

  override def getFileSize: Option[Long] = {
    fileSize
  }

  override def getFileCount: Option[Long] = {
    fileCount
  }

  override def getModificationTime(context: FlamyContext, refresh: Boolean = false): Option[Long] = {
    modificationTime
  }

  override def toString: String = {
    name.toString
  }

}

object SchemaWithInfo {

  val getSparkSchema: StructType = {
    StructType(Seq(
      StructField("schema", StringType),
      StructField("num_tables", LongType),
      StructField("size", LongType),
      StructField("num_files", LongType),
      StructField("modification_time", LongType)
    ))
  }

  def getInfoHeader: Seq[String] = {
    getSparkSchema.fields.map{_.name}
  }

}
