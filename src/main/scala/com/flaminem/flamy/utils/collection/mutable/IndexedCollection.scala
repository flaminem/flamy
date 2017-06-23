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

import scala.collection.immutable.{SortedSet, TreeMap}
import scala.language.implicitConversions

/**
 * Created by fpin on 10/27/14.
 */
// TODO: IndexedCollection should be immutable too?
// TODO: Once everything is migrated to Java, try to make this class a trait with : http://stackoverflow.com/questions/14483732/defining-implicit-view-bounds-on-scala-traits
abstract class IndexedCollection[I: Ordering,V](protected var map: TreeMap[I, V]) extends Traversable[V] {

  def this() {
    this(new TreeMap[I,V]())
  }

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

  def filterValues(f: (V) => Boolean): TreeMap[I, V] = map.filter{case (i,v) => f(v)}

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
  def indices: SortedSet[I] = {
    map.keySet
  }

  /**
   * Adds a value to the collection and overrides the previous entry if it was present.
   * @param value
   * @return
   */
  def add(value: V) : Boolean = {
    val index: I = getIndexOf(value)
    map = map + ((index, value))
    true
  }

  def addAll(values: TraversableOnce[V]): IndexedCollection[I, V] = values.foldLeft(this)(_+=_)

  def addAll(values: Iterable[V]): IndexedCollection[I, V] = values.foldLeft(this)(_+=_)

  def remove(index: I) : Boolean =
    map.get(index) match {
      case None => false
      case Some(_) =>
        map = map - index
        true
    }

  /**
   * Removes from the set the entry with the same index as the specified value,
   * and returns its value if such entry exists, or <i>null</i> if no match is found.
   * The returned object is not necessarily the same as the one given in argument.
   * @param value
   * @return
   */
  def removeValue(value: V): Boolean = remove(getIndexOf(value))

  def +=[V2<:V](value: V2): IndexedCollection[I, V] = {add(value) ; this}

  def ++=[L<:TraversableOnce[V]](values: L): IndexedCollection[I, V] = values.foldLeft(this){_+=_}

//  def -[V2<:V](value: V2) = removeValue(value)


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

  def clear(): Unit = { map = new TreeMap() }

  override def toString: String = f"[${map.values.mkString(", ")}]"

  override def size: Int = map.size

  override def foreach[U](f: (V) => U): Unit = {map.values.foreach(f)}

}


