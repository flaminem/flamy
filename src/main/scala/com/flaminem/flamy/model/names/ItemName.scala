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

import com.flaminem.flamy.model.exceptions.FlamyException
import org.rogach.scallop.ValueConverter

import scala.collection.TraversableLike
import scala.collection.generic.CanBuildFrom
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

/**
 * Created by fpin on 11/3/14.
 */
trait ItemName extends Ordered[ItemName] {

  val fullName : String
  def isInOrEqual(that: ItemName): Boolean

  def canEqual(other: Any): Boolean = other.isInstanceOf[ItemName]

  override def equals(other: Any): Boolean = {
    other match {
      case that: ItemName => (that canEqual this) && fullName == that.fullName
      case _ => false
    }
  }

  override def hashCode(): Int = {
    fullName.hashCode
  }

  override def compare(that: ItemName): Int = {
    this.fullName.compareTo(that.fullName)
  }

  override def toString: String = {
    fullName
  }
}

object ItemName {

  def apply(s: String): ItemName = tryParse(s).get

  implicit def stringToItemName(s: String): ItemName = tryParse(s).get

  def parse(s: String): Option[ItemName] = {
    tryParse(s).toOption
  }

  /**
    * Try to create an ItemName by parsing a string.
    * Return a Failure if the string could not be correctly parsed.
    * @param s
    * @return
    */
  def tryParse(s: String): Try[ItemName] = {
    Try{
      SchemaName.parse(s)
      .orElse{
        TableName.parse(s)
      }
      .orElse{
        TablePartitionName.parse(s)
      }
      .getOrElse {
        throw new IllegalArgumentException("Wrong item name : " + s)
      }
    }
  }

  def tryParse(s: String, allowedTypes: Set[Class[_]]): Try[ItemName] = {
    tryParse(s).flatMap {
      case item if allowedTypes.contains(item.getClass) => Success(item)
      case item =>
        Failure(
          new IllegalArgumentException(
            s"Item $item is a ${item.getClass.getSimpleName}, " +
            s"but only the following item types are allowed: ${allowedTypes.map{_.getSimpleName}.mkString("[", ", ", "]")}"
          )
        )
    }
  }

  implicit def fromStringTraversableLike[T[Z] <: TraversableLike[Z, T[Z]]]
  (l: T[String])(implicit bf: CanBuildFrom[T[String], ItemName, T[ItemName]]) : T[ItemName] = {
    l.map{tryParse(_).get}
  }

  private def fromArgs(args: Seq[String]): Either[String, Option[List[ItemName]]] = {
    val tries: Seq[Try[ItemName]] = args.map{tryParse}
    if(tries.forall{_.isSuccess}){
      Right(Some(tries.map{_.get}.toList))
    }
    else {
      val firstFailureIndex = tries.indexWhere(_.isFailure)
      Left(s"Could not parse the item name ${args(firstFailureIndex)}")
    }
  }

  implicit val scallopConverterList: ValueConverter[List[ItemName]] = {
    new ValueConverter[List[ItemName]] {
      override def parse(s: List[(String, List[String])]): Either[String, Option[List[ItemName]]] = {
        s match {
          case l if l.nonEmpty => fromArgs(l.flatMap{_._2})
          case Nil => Right(None)
        }
      }
      override val tag: TypeTag[List[ItemName]] = typeTag[List[ItemName]]
      override val argType = org.rogach.scallop.ArgType.LIST
    }
  }

}