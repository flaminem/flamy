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

import scala.collection.immutable.{SortedSet, TreeMap}
import scala.language.implicitConversions

abstract class IndexedCollection[I: Ordering, V, +Repr](
  protected val map: TreeMap[I, V]
) extends Traversable[V] {

  /**
    * Returns a new instance of the collection, containing the specified map
    */
  def copy(map: TreeMap[I, V]): Repr

  def getIndexOf(value: V): I

  /**
   * Returns the value with the matching index, or null if no match is found.
   * @param index
   * @return
   */
  def get(index: I): Option[V] =  map.get(index)

  /**
   * Returns the value with the matching index, or null if no match is found.
   * @param index
   * @return
   */
  def getOrElse(index: I, default: V): V = map.getOrElse(index,default)

  /**
   * Return a collection of all stored Objects
   * @return
   */
  def getAllValues: Iterable[V] = {
    map.values
  }

  /**
   * Return the set of all stored indices
   * @return
   */
  def indices: SortedSet[I] = map.keySet

  def contains(index: I): Boolean = {
    map.contains(index)
  }

  /**
   * Returns true if the set contains an entry with the same index as the specified value.
   * The stored value is not necessarily the same as the one given in argument.
   * @param value
   * @return
   */
  def containsValue(value: V): Boolean = {
    map.contains(getIndexOf(value))
  }

  def +(value: V): Repr = {
    copy(map + (getIndexOf(value) -> value))
  }

  def ++(values: Traversable[V]): Repr = {
    copy(map ++ values.map{v => getIndexOf(v) -> v})
  }

  def -(value: V): Repr = {
    copy(map - getIndexOf(value))
  }

  override def toString: String = f"[${map.values.mkString(", ")}]"

  override def size: Int = map.size

  override def foreach[U](f: (V) => U): Unit = {map.values.foreach(f)}

}


