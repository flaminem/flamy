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

package com.flaminem.flamy.utils.collection.mutable

import com.flaminem.flamy.utils.Named

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.{Builder, ListBuffer}
import scala.collection.{TraversableLike, mutable}
import scala.language.implicitConversions

/**
 * Created by fpin on 10/30/14.
 */
class NamedCollection[V <: Named] extends IndexedCollection[String, V] with TraversableLike[V, NamedCollection[V]] {
  override def newBuilder: mutable.Builder[V, NamedCollection[V]] = {
    new ListBuffer[V] mapResult{x => new NamedCollection(x)}
  }

  def this(namedItems: Traversable[V]) = {
    this
    this++=namedItems
  }

  def getIndexOf(value: V): String = value.getName.toLowerCase

}

object NamedCollection {

  def newBuilder[V <: Named](): Builder[V, NamedCollection[V]] = {
    new Builder[V, NamedCollection[V]]() {
      val buffer = new NamedCollection[V]()

      override def +=(elem: V): this.type = {
        buffer += elem
        this
      }

      override def clear(): Unit = {
        buffer.clear()
      }

      override def result(): NamedCollection[V] = {
        buffer
      }
    }
  }

  implicit def canBuildFrom[In <: Named, Out <: Named]: CanBuildFrom[NamedCollection[In], Out, NamedCollection[Out]] = {
    new CanBuildFrom[NamedCollection[In], Out, NamedCollection[Out]]() {
      override def apply(from: NamedCollection[In]): Builder[Out, NamedCollection[Out]] = newBuilder()
      override def apply(): Builder[Out, NamedCollection[Out]] = newBuilder()
    }
  }

}

