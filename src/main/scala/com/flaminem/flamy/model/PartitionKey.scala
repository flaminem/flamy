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

import com.flaminem.flamy.model.columns.NoValue

/**
  * A partition key (name),
  * ex: "day=2014-03-03" is a PartitionColumn
  *     "day" is a PartitionKey
  *
  * The class PartitionKey extends the class PartitionColumn as it is basically a PartitionColumn with no value.
  */
class PartitionKey (
  override val rawColumnName: String,
  _columnType: Option[String],
  override val comment: Option[String]
)
extends PartitionColumn(rawColumnName, _columnType, comment, NoValue) {

  def this(col: Column) {
    this(col.columnName, col.columnType, col.comment)
  }

  def this(name: String) {
    this(name, None, None)
  }

}
