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

import com.flaminem.flamy.utils.Mergeable

import scala.collection.immutable.TreeMap

/**
 * Created by fpin on 10/30/14.
 */
trait MergeableIndexedCollection[I, V <: Mergeable[V], Repr] extends IndexedCollection[I, V, Repr] {

  private def addValueToMap(value: V, map: TreeMap[I, V]): TreeMap[I, V] = {
    val index: I = getIndexOf(value)
    map.get(index) match {
      case None => map + (index -> value)
      case Some(v) => map + (index -> v.merge(value))
    }
  }

  override def +(value: V): Repr = {
    copy(addValueToMap(value, map))
  }

  override def ++(values: Traversable[V]): Repr = {
    copy(values.foldRight(map){addValueToMap})
  }

}


