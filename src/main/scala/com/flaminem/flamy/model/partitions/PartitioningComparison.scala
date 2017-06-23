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


object PartitioningComparison {

  /**
    * The result of the comparePartitioning method.
    */
  sealed trait PartitioningComparison {

    def isEqual: Boolean = false
    def isGreater: Boolean = false
    def isSmaller: Boolean = false
    def isDifferent: Boolean = false
    def isMissing: Boolean = false

  }

  /**
    * The tables have the same partitioning
    */
  case object Equal extends PartitioningComparison {
    override def isEqual: Boolean = true
  }

  /**
    * The left table has more levels of partitioning
    *
    * @param commonCols the columns present in both tables
    */
  case class Greater(commonCols: Seq[String]) extends PartitioningComparison {
    override def isGreater: Boolean = true
  }

  /**
    * The right table has more levels of partitioning
    *
    * @param commonCols the columns present in both tables
    */
  case class Smaller(commonCols: Seq[String]) extends PartitioningComparison {
    override def isSmaller: Boolean = true
  }

  /**
    * The tables have different partitioning schemes, no inclusion could be found.
    */
  case class Different(commonCols: Seq[String]) extends PartitioningComparison {
    override def isDifferent: Boolean = true
  }

  /**
    * Information for at least one of the tables was missing
    */
  case object Missing extends PartitioningComparison {
    override def isMissing: Boolean = true
  }

}
