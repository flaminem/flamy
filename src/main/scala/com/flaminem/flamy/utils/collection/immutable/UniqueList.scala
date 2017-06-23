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

package com.flaminem.flamy.utils.collection.immutable

import scala.collection.generic.{CanBuildFrom, SeqForwarder}
import scala.collection.mutable.{Builder, ListBuffer}
import scala.collection.{SeqLike, mutable}

/**
  * A list of element where unicity is ensured.
  * Throws a UnicityViolationException if the same element is added twice to the collection.
  */
class UniqueList[T] private (
  list: List[T],
  set: Set[T]
) extends Seq[T] with SeqForwarder[T] with SeqLike[T, UniqueList[T]] {

  override def newBuilder: Builder[T, UniqueList[T]] = UniqueList.canBuildFrom()

  override def underlying: Seq[T] = list

  override def toString(): String = s"UniqueList(${list.mkString(", ")})"
}

object UniqueList {

  def newBuilder[T](): Builder[T, UniqueList[T]] = {
    new Builder[T, UniqueList[T]]() {
      val listBuffer = new ListBuffer[T]()
      val setBuffer = new mutable.HashSet[T]()

      override def +=(elem: T): this.type = {
        if(setBuffer.contains(elem)){
          throw new UnicityViolationException(elem)
        }
        else {
          listBuffer += elem
          setBuffer += elem
        }
        this
      }

      override def clear(): Unit = {
        listBuffer.clear()
        setBuffer.clear()
      }

      override def result(): UniqueList[T] = {
        new UniqueList(listBuffer.toList, setBuffer.toSet)
      }
    }
  }

  implicit def canBuildFrom[In, Out]: CanBuildFrom[UniqueList[In], Out, UniqueList[Out]] = {
    new CanBuildFrom[UniqueList[In], Out, UniqueList[Out]]() {
      override def apply(from: UniqueList[In]): Builder[Out, UniqueList[Out]] = newBuilder()
      override def apply(): Builder[Out, UniqueList[Out]] = newBuilder()
    }
  }

  def apply[T](elems: T*): UniqueList[T] = {
    (newBuilder() ++= elems).result()
  }

  def apply[T](elems: Iterable[T]): UniqueList[T] = {
    (newBuilder() ++= elems).result()
  }

}
