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

import com.flaminem.flamy.utils.Mergeable

/**
 * Created by fpin on 10/30/14.
 */
trait MergeableIndexedCollection[I, V <: Mergeable[V]] extends IndexedCollection[I, V] {

  override def add(value: V): Boolean = {
    val index: I = getIndexOf(value)
    super.get(index) match {
      case None => super.add(value)
      case Some(v) => super.add(v.merge(value))
    }
    true
  }

}


