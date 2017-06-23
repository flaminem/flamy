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

import com.flaminem.flamy.model.PartitionColumn

import scala.collection.mutable.ListBuffer
import scala.collection.{SeqLike, mutable}

/**
 * Created by fpin on 1/29/15.
 */
class Partition(override val columns: Seq[PartitionColumn])
extends TPartition
with SeqLike[PartitionColumn, Partition]
{

  override def newBuilderImpl: mutable.Builder[PartitionColumn, Partition] = {
    new ListBuffer[PartitionColumn] mapResult (x => new Partition(x))
  }

  override def newBuilder: mutable.Builder[PartitionColumn, Partition] = {
    newBuilderImpl
  }

  def this(partitionName: String) {
    this(
      partitionName.split("/").map {
        s =>
          val a = s.split("=")
          new PartitionColumn(a(0), Option(a(1)))
      }
    )
  }

}
