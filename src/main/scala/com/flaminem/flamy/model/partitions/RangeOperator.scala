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
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.utils.macros.SealedValues

/**
  * Created by fpin on 7/26/16.
  */
sealed trait RangeOperator {

  val name: String

  def apply(left: PartitionColumn, right: PartitionColumn): Boolean = {
    assert(left.columnName == right.columnName)
    assert(left.value.isDefined)
    assert(right.value.isDefined)
    val columnType: String = left.columnType.getOrElse(right.columnType.getOrElse("string"))
    apply(left.value.get, right.value.get, columnType)
  }

  protected def apply(left: String, right: String, columnType: String): Boolean

}

object RangeOperator {

  case object Equal extends RangeOperator{
    override val name: String = "="
    override def apply(left: String, right: String, columnType: String): Boolean = {
      columnType match {
        case "tinyint" | "smallint" | "int" | "bigint" => left.toInt == right.toInt
        case _ => left == right
      }
    }

  }

  case object Lower extends RangeOperator{
    override val name: String = "<"
    override def apply(left: String, right: String, columnType: String): Boolean = {
      columnType match {
        case "tinyint" | "smallint" | "int" | "bigint" => left.toInt < right.toInt
        case _ => left < right
      }
    }
  }

  case object LowerOrEqual extends RangeOperator{
    override val name: String = "<="
    override def apply(left: String, right: String, columnType: String): Boolean = {
      columnType match {
        case "tinyint" | "smallint" | "int" | "bigint" => left.toInt <= right.toInt
        case _ => left <= right
      }
    }
  }

  case object Greater extends RangeOperator{
    override val name: String = ">"
    override def apply(left: String, right: String, columnType: String): Boolean = {
      columnType match {
        case "tinyint" | "smallint" | "int" | "bigint" => left.toInt > right.toInt
        case _ => left > right
      }
    }
  }

  case object GreaterOrEqual extends RangeOperator{
    override val name: String = ">="
    override def apply(left: String, right: String, columnType: String): Boolean = {
      columnType match {
        case "tinyint" | "smallint" | "int" | "bigint" => left.toInt >= right.toInt
        case _ => left >= right
      }
    }
  }

  /* This line must stay after the value declaration or it will be empty */
  val values: Seq[RangeOperator] = SealedValues.values[RangeOperator]

  def apply(string: String): RangeOperator = {
    values.find(_.name == string) match {
      case Some(op) => op
      case None =>
        throw new FlamyException(
          s"This string ($string) could not be recognized as an operator. " +
          s"Valid operators are ${values.mkString(" ")}")
    }
  }

}
