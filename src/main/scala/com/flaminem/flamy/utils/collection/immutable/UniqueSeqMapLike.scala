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

/**
  * An immutable map that:
  *  - Throws a UnicityViolationException if a key is added more than once to the collection.
  *  - Preserves the insertion order when iterating over the key, values or key-value pairs
  */
trait UniqueSeqMapLike[K, V, +Repr] extends Traversable[(K, V)] {

  def entries: Seq[(K, V)]
  def map: Map[K, V]

  /**
    * Returns a new instance of the collection, build with the specified entries and map.
    * @param entries
    * @param map
    * @return
    */
  def copy(entries: Seq[(K, V)], map: Map[K, V]): Repr

  private def mapAdd(m: Map[K, V], kv: (K, V)): Map[K, V] = {
    if(m.contains(kv._1)){
      throw new UnicityViolationException(kv)
    }
    else{
      m + kv
    }
  }

  def +(kv: (K, V)): Repr = {
    copy(entries :+ kv, mapAdd(map, kv))
  }

  def ++(pairs: Traversable[(K, V)]): Repr = {
    copy(entries ++ pairs, pairs.foldLeft(map){mapAdd})
  }

  def apply(key: K): Option[V] = {
    map.get(key)
  }

  def get(key: K): Option[V] = {
    map.get(key)
  }

  def contains(key: K): Boolean = {
    map.contains(key)
  }

  def getOrElse(key: K, replacement: V): V = {
    map.getOrElse(key, replacement)
  }

  def keys: Iterable[K] = {
    entries.view.map{_._1}
  }

  def values: Iterable[V] = {
    entries.view.map{_._2}
  }

  override def toString(): String = s"${this.getClass.getSimpleName}(${entries.mkString(", ")})"

  override def size: Int = map.size

  override def foreach[U](f: ((K,V)) => U): Unit = {
    entries.foreach(f)
  }

}

