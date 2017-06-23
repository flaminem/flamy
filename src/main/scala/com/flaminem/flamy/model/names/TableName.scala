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
import org.rogach.scallop.ValueConverter

import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.util.matching.Regex

/**
 * The full name given to a table
 * (eg: "stats.daily_visitors")
  *
  * @param fullName
 */
class TableName private (val fullName: String) extends ItemName {
  lazy val schemaName: SchemaName = SchemaName(fullName.split('.')(0))
  lazy val name: String = fullName.split('.')(1)

  def isInSchema(schema: ItemName): Boolean = schema match {
    case s: SchemaName => s.equals(schemaName)
    case default => false
  }
  override def isInOrEqual(that: ItemName): Boolean = that match {
    case name: SchemaName => this.isInSchema(name)
    case name: TableName => name==this
    case _ => false
  }
}

object TableName {

  // TODO: during parsing, table names with wrong names are allowed. We should probably add some safety about that.
  def apply(fullName: String): TableName = {
    new TableName(fullName.toLowerCase)
  }

  def unapply(tableName: TableName): Option[String] = Some(tableName.fullName)

  def apply(schemaName: String, tableName: String): TableName = new TableName(schemaName.toLowerCase + "." + tableName.toLowerCase)

  def apply(schemaName: SchemaName, tableName: String): TableName = new TableName(schemaName.fullName + "." + tableName.toLowerCase)

  implicit val order: Ordering[TableName] = new Ordering[TableName]{
    override def compare(x: TableName, y: TableName): Int = x.fullName.compareTo(y.fullName)
  }

  val t: String = ParsingUtils.t
  val tableRegex: Regex = s"\\A$t[.]$t\\z".r

  def parse(s: String): Option[TableName] = {
    s match {
      case tableRegex() => Some(new TableName(s.toLowerCase))
      case _ => None
    }
  }

  private def fromArg(arg: String): Either[String, Option[TableName]] = {
    val res: Option[TableName] = parse(arg)
    if(res.isDefined){
      Right(Some(res.get))
    }
    else {
      Left("")
    }
  }

  private def fromArgs(args: Seq[String]): Either[String, Option[List[TableName]]] = {
    val tries: Seq[Option[TableName]] = args.map{parse}
    if(tries.forall{_.isDefined}){
      Right(Some(tries.map{_.get}.toList))
    }
    else {
      Left("")
    }
  }

  implicit val scallopConverter: ValueConverter[TableName] = {
    new ValueConverter[TableName] {
      override def parse(s: List[(String, List[String])]): Either[String, Option[TableName]] = {
        s match {
          case l if l.nonEmpty => fromArg(l.flatMap{_._2}.head)
          case Nil => Right(None)
        }
      }
      override val tag: TypeTag[TableName] = typeTag[TableName]
      override val argType = org.rogach.scallop.ArgType.SINGLE
    }
  }

  implicit val scallopConverterList: ValueConverter[List[TableName]] = {
    new ValueConverter[List[TableName]] {
      override def parse(s: List[(String, List[String])]): Either[String, Option[List[TableName]]] = {
        s match {
          case l if l.nonEmpty => fromArgs(l.flatMap{_._2})
          case Nil => Right(None)
        }
      }
      override val tag: TypeTag[List[TableName]] = typeTag[List[TableName]]
      override val argType = org.rogach.scallop.ArgType.LIST
    }
  }

  implicit def fromString(s: String): TableName = TableName(s)

}