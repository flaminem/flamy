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

package com.flaminem.flamy.model.names

import com.flaminem.flamy.model.{PartitionColumn, PartitionKey}

/**
 * The full name given to one column of a partition
 * (eg: "day=2014-10-12")
 *
 * @param fullName
 */
case class PartitionColumnName(fullName: String) extends ItemName {
  lazy val key: String = fullName.split('=')(0).toLowerCase
  lazy val value: String = fullName.split('=')(1)
  override def isInOrEqual(that: ItemName): Boolean = that match {
    case name: PartitionColumnName => name==this
    case _ => false
  }
  def toPartitionColumn: PartitionColumn = new PartitionColumn(key, Option(value))
  def toPartitionKey: PartitionKey = new PartitionKey(key)
}
