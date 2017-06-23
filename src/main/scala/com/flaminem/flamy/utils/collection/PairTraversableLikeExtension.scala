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

package com.flaminem.flamy.utils.collection

import scala.collection.TraversableLike
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.Map
import scala.language.higherKinds
import scala.util.Sorting

/**
  * Provide a few functions to use on traversable of pairs.
  */
class PairTraversableLikeExtension[K, V, T[Z] <: TraversableLike[Z, T[Z]]]
(t: T[(K,V)]) (implicit bf: CanBuildFrom[T[(K, V)], V, T[V]]) {

  /**
    * Transform a collection of pairs Col[(K,V)] to a Map[K,Col[V]]
    * where all values with identical keys are grouped.
    * Example:
    * List((1,'a'),(1,'b'),(2,'c')).groupByKey
    * > Map(2 -> List(c), 1 -> List(a, b))
    * @return
    */
  def groupByKey: Map[K,T[V]] = {
    t.groupBy(_._1).mapValues{_.map(x=>x._2)}
  }

  /**
    * Transform a collection of pairs Col[(K,V)] to an Array[K,Col[V]]
    * where all values with identical keys are grouped and the keys are sorted.
    * Example:
    * List((1,'a'),(1,'b'),(2,'c')).groupBySortedKey
    * > Array((1,List(a, b)), (2,List(c)))
    * @return
    */
  def groupBySortedKey(implicit ord: Ordering[K]): Array[(K,T[V])] = {
    t.groupByKey.sortByKey
  }


  def sortByKey(implicit ord: Ordering[K]): Array[(K, V)] = {
    val arr: Array[(K, V)] = t.toArray[(K,V)]
    Sorting.quickSort[(K,V)](arr)(Ordering.by[(K, V), K](x => x._1))
    arr
  }


}

