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
import com.flaminem.flamy.utils.time.TimeUtils
import com.flaminem.flamy.utils.units.UnitBytes

trait ItemWithInfo {

  def creationTime: Option[Long]

  val location: String

  def getFileSize: Option[Long]

  def getFileCount: Option[Long]

  /**
    * Get the modification time of the files associated to this item.
    *
    * Warning: for a table, this corresponds to the modification time of the corresponding
    * folder on hdfs, which means the last time a partition folder was added or removed if the table is partitioned,
    * or the last time data were changed if the table is not partitioned.
    *
    * @param context
    * @param refresh
    * @return
    */
  def getModificationTime(context: FlamyContext, refresh: Boolean = false): Option[Long]

  def getFormattedInfo(context: FlamyContext, humanReadable: Boolean): Seq[String] = {
    Seq(formattedFileSize(context, humanReadable), formattedFileCount(context), formattedModificationTime(context))
  }

  def formattedFileSize(context: FlamyContext, humanReadable: Boolean): String = {
    getFileSize.map {
      case size if humanReadable => UnitBytes(size).toString()
      case size => size.toString
    }.getOrElse("")
  }

  def formattedFileCount(context: FlamyContext): String = {
    getFileCount.map{_.toString}.getOrElse("")
  }

  def formattedModificationTime(context: FlamyContext): String = {
    getModificationTime(context).map{
      time => TimeUtils.timestampToUniversalTime(time)
    }.getOrElse("")
  }

}

