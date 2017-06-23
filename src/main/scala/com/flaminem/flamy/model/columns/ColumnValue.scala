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

package com.flaminem.flamy.model.columns

import com.flaminem.flamy.model.exceptions.UnexpectedBehaviorException
import com.flaminem.flamy.parsing.model.ColumnDependency

import scala.util.matching.Regex

/**
  * Represents the value stored in a Column.
  * It can be a constant, an expression, or nothing
  */
trait ColumnValue {

  def isEmpty: Boolean = false
  final def isDefined: Boolean = !isEmpty

  // TODO: this method is temporary, only for backwards compatibility
  def get: String

}

object ColumnValue {

  private val partitionVariableRE: Regex = """\A[$][{]partition:(.*)[}]\z""".r
  private val variableRE: Regex = """\A([$][{].*[}])\z""".r

  def apply(string: Option[String]): ColumnValue = {
    string match {
      case None => NoValue
      case Some(partitionVariableRE(v)) => new ColumnDependency(v)
      case Some(variableRE(v)) => VariableValue(v)
      case Some(v) => ConstantValue(v)
    }
  }

  def apply(string: String): ColumnValue = {
    apply(Option(string))
  }

}

case object NoValue extends ColumnValue {

  override def isEmpty: Boolean = true

  override def get: String = throw new UnexpectedBehaviorException()
}

case class ConstantValue private[columns](value: String) extends ColumnValue {

  override def toString: String = value

  override def get: String = value
}

case class VariableValue private[columns](value: String) extends ColumnValue {

  override def toString: String = value

  override def get: String = value

}