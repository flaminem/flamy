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
import org.rogach.scallop.ValueConverter

/**
  * Created by fpin on 7/27/16.
  */
class PartitionFilter(partitionColumnFilters: Seq[PartitionColumnFilter]) {

  private val groupedPCFilters: Map[String, Map[RangeOperator, Seq[PartitionColumnFilter]]] = {
    partitionColumnFilters
      .groupBy{_.partitionColumn.columnName}
      .map{case (key, filters) => (key, filters.groupBy{_.operator})}
  }

  /**
    * Check that the partition is accepted by this filter.
    * @param partition
    * @return
    */
  def apply(partition: TPartition): Boolean = {
    val matches =
      for {
        pc: PartitionColumn <- partition.view
        group <- groupedPCFilters.get(pc.columnName).view
        (op: RangeOperator, filters: Seq[PartitionColumnFilter]) <- group
      } yield {
        if (op == RangeOperator.Equal) {
          filters.exists{f => f(pc)}
        }
        else {
          filters.forall{f =>
            f(pc)
          }
        }
      }
    matches.forall{_ == true}
  }

  override def toString: String = {
    partitionColumnFilters.mkString(" ")
  }

}

object PartitionFilter {

  def fromArgs(desc: Seq[String]): Option[PartitionFilter] = {
    val filters = desc.flatMap{PartitionColumnFilter.fromString(_)}
    if(filters.size==desc.size){
      Some(new PartitionFilter(filters))
    }
    else {
      None
    }
  }

  implicit val scallopConverter = {
    new ValueConverter[PartitionFilter] {
      override def parse(s: List[(String, List[String])]): Either[String, Option[PartitionFilter]] = {
        s match {
          case (_, l) :: Nil => Right(fromArgs(l))
          case Nil => Right(None)
          case _ => Left("Please provide a partition filter. The following operators are recognized: = > >= < <= ")
        }
      }

      override val tag = scala.reflect.runtime.universe.typeTag[PartitionFilter]
      override val argType = org.rogach.scallop.ArgType.LIST
    }
  }

}
