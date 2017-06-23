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

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model.{IOFormat, PartitionColumn}

/**
 * Created by fpin on 1/18/15.
 */
class DummyPartitionWithInfo private (
  override val location: String,
  private val fileSize: Long,
  override val creationTime: Option[Long],
  private val modificationTime: Long,
  columns: PartitionColumn*
)
extends Partition(columns) with PartitionWithInfo {

  override val ioFormat: IOFormat = {
    new IOFormat("", "", "")
  }

  def this(location: String, fileSize: Long, creationTime: Long, modificationTime: Long, columns: PartitionColumn*) {
    this(location, fileSize, Some(creationTime), modificationTime, columns: _*)
  }

  override def getFileSize: Option[Long] = {
    Some(fileSize)
  }

  override def getFileCount: Option[Long] = {
    None
  }

  override def getModificationTime(context: FlamyContext, refresh: Boolean = false): Option[Long] = {
    Some(modificationTime)
  }


}
