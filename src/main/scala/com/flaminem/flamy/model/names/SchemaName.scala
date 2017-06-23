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

import com.flaminem.flamy.parsing.ParsingUtils

import scala.language.implicitConversions
import scala.util.matching.Regex

/**
 * The full name given to a schema
 * (eg: "stats")
  *
  * @param fullName
 */
class SchemaName private (val fullName: String) extends ItemName {
  def name: String = fullName
  override def isInOrEqual(that: ItemName): Boolean = that match {
    case name: SchemaName => name==this
    case _ => false
  }
}
object SchemaName {

  def apply(fullName: String): SchemaName = {
    parse(fullName).getOrElse{
      throw new IllegalArgumentException(s"$fullName is not a correct SchemaName")
    }
  }

  val t: String = ParsingUtils.t
  val schemaRegex: Regex = s"\\A$t\\z".r

  def parse(s: String): Option[SchemaName] = {
    s match {
      case schemaRegex() => Some(new SchemaName(s))
      case _ => None
    }
  }

  def unapply(schemaName: SchemaName): Option[String] = Some(schemaName.fullName)

  implicit val order: Ordering[SchemaName] = new Ordering[SchemaName]{
    override def compare(x: SchemaName, y: SchemaName): Int = x.fullName.compareTo(y.fullName)
  }

  implicit def toString(schema: SchemaName): String = schema.fullName

}