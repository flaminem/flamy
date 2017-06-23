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

import com.flaminem.flamy.model.columns.ColumnValue

/**
  * Created by fpin on 9/13/16.
  */
class TransformedPartitionColumn private (
  override val rawColumnName: String,
  _columnType: Option[String],
  override val comment: Option[String],
  override val value: ColumnValue,
  val previous: PartitionColumn
)
extends PartitionColumn(rawColumnName, _columnType, comment, value) {

  def this(pc: PartitionColumn, newName: String) {
    this(newName, pc.columnType, pc.comment, pc.value, pc)
  }

}
