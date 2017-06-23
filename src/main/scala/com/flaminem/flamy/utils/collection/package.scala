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

package com.flaminem.flamy.utils

import scala.collection.TraversableLike
import scala.collection.generic.CanBuildFrom
import scala.language.{higherKinds, implicitConversions}

/**
  * Created by fpin on 8/20/16.
  */
package object collection {

  implicit def toPairTraversableLikeExtension[K, V, T[Z] <: TraversableLike[Z, T[Z]]]
  (t: T[(K,V)]) (implicit bf: CanBuildFrom[T[(K,V)], V, T[V]]): PairTraversableLikeExtension[K, V, T] = {
    new PairTraversableLikeExtension[K,V,T](t)
  }

  implicit def toTraversableLikeExtension[A, T[A] <: TraversableLike[A, T[A]]] (t: T[A]) : TraversableLikeExtension[A, T] = {
    new TraversableLikeExtension[A, T](t)
  }

}
