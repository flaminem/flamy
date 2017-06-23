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

import com.flaminem.flamy.model.{PartitionColumn, PartitionKey}

import scala.collection.JavaConversions._
import scala.collection.{SeqLike, mutable}

/** This trait is used to represent a Hive partition or group of partitions, represented by a set of PartitionColumns
  * For instance, if a table is partitioned by "day" and "type", the partition "day=2014-01-01"
  * represents the group of all the partitions of the form "day=2014-01-01/type=*"
  *
  * This is basically a Collection of PartitionColumns, with a natural lexicographic ordering,
  * and methods to compare inclusions with other Partition.
  */
trait TPartition
extends Ordered[TPartition]
with Seq[PartitionColumn]
with SeqLike[PartitionColumn, TPartition] {

  val columns: Seq[PartitionColumn]

  /**
    * Due to some strange wibbily wobbly timey wimey scala stuff, the newBuilder method must be concrete
    * We thus ask the classes extending this trait to provide an implementation newBuilderImpl, and to also
    * override the newBuilder method. Anything less ugly that works is welcome.
    * */
  def newBuilderImpl: mutable.Builder[PartitionColumn, TPartition]
  override def newBuilder: mutable.Builder[PartitionColumn, TPartition] = newBuilderImpl

  override def length: Int = columns.length
  override def apply(idx: Int): PartitionColumn = columns.apply(idx)
  override def iterator: Iterator[PartitionColumn] = columns.iterator

  lazy val columnSet: Set[PartitionColumn] = columns.toSet

  def columnNames: Seq[String] = columns.map{_.columnName}

  def keys: Seq[String] = columns.map{_.columnName}

  def partitionKeys: Seq[PartitionKey] = columns.map{new PartitionKey(_)}

  def values: Seq[String] = columns.map{_.value.get}

  //TODO: create a PartitionName type ?
  def partitionName: String = {
    columns.mkString("/")
  }

  override def toString(): String = {
    partitionName
  }

  override def compare(that: TPartition): Int = {
    this.toString().compareTo(that.toString())
  }

  override def equals(other: Any): Boolean = other match {
    case that: TPartition => this.columnSet == that.columnSet
    case _ => false
  }

  override def hashCode(): Int = {
    columnSet.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  /**
    * Compare partition schemes between this table's partition and another table's partition
    *
    * @param that
    * @return Equal if the two tables have the same partition scheme
    *         Greater if the left table partition keys contains the right table partitions keys
    *         Smaller if the left table partition keys are contained in the right table partition keys
    *         Different if neither of the above inclusion is true
    *         Missing if the required partitioning information was not found
    */
  def comparePartitionScheme(that: TPartition): PartitioningComparison.PartitioningComparison = {
    (this.columnNames, that.columnNames) match {
      case (s, t) if s == t => PartitioningComparison.Equal
      case (s, t) if s.containsAll(t) => PartitioningComparison.Greater(s.intersect(t))
      case (s, t) if t.containsAll(s) => PartitioningComparison.Smaller(s.intersect(t))
      case (s, t) => PartitioningComparison.Different(s.intersect(t))
    }
  }

  /**
    * Return true if this partition is contained in that partition.
    * For example :
    * "part1=A/part2=B".isInOrEqual("part1=A") == true
    * "part1=A".isInOrEqual("part1=A") == true
    *
    * "part1=A".isInOrEqual("part2=B") == false
    * "part1=A".isInOrEqual("part1=A/part2=B") == false
    *
    * @param that
    * @return
    */
  def isInOrEqual(that: TPartition): Boolean = {
    this.columns.containsAll(that.columns)
  }

}

