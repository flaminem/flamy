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
import com.flaminem.flamy.model.PartitionKey
import com.flaminem.flamy.model.names.TableName

/**
  * Created by fpin on 8/10/16.
  */
class DummyTablePartitioningInfo(
  override val tableName: TableName,
  override val partitionKeys: Seq[PartitionKey],
  override val tablePartitions: Seq[DummyPartitionWithInfo]
) extends TablePartitioningInfo(tableName, partitionKeys, tablePartitions){

  /**
    * We override this to do nothing.
    * @param context
    * @return this
    */
  override def refreshAllFileStatus(context: FlamyContext): TablePartitioningInfo = {
    this
  }

}
