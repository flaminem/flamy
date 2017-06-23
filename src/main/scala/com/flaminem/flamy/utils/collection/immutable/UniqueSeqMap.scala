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
  * Created by fpin on 11/23/16.
  */
class UniqueSeqMap[K, V](
  val entries: Seq[(K, V)] = Seq[(K, V)](),
  val map: Map[K, V] = Map[K, V]()
) extends UniqueSeqMapLike[K, V, UniqueSeqMap[K, V]]{

  override def copy(entries: Seq[(K, V)], map: Map[K, V]): UniqueSeqMap[K, V] = {
    new UniqueSeqMap(entries, map)
  }

}

object UniqueSeqMap {

  def apply[K, V](pairs: (K, V)*): UniqueSeqMap[K, V] = {
    new UniqueSeqMap[K, V]() ++ pairs
  }

  def apply[K, V](pairs: Iterable[(K, V)]): UniqueSeqMap[K, V] = {
    new UniqueSeqMap[K, V]() ++ pairs
  }

}
