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
import scala.collection.mutable.ListBuffer
import scala.language.higherKinds

/**
  * Created by fpin on 12/20/16.
  */
class TraversableLikeExtension[A, T[A] <: TraversableLike[A, T[A]]](t: T[A]) {

  /**
    * Split a list according to the specified function and preserves the original ordering.
    * Each time the output of the function changes, a new group is created.
    * Two elements with the same function image can thus be in different groups if they are separated by an element with a different image.
    *
    * @example {{{
    * val l: Seq[Int] = 1 to 10
    * l.splitBy{x => x / 6}
    * > Seq((0, Vector(1, 2, 3, 4, 5)), (1, Vector(6, 7, 8, 9, 10))
    *
    * l.splitBy{x => x % 6 == 0}
    * > Seq((false, Vector(1, 2, 3, 4, 5)), (true, Vector(6)), (false, Vector(7, 8, 9, 10)))
    * }}}
    * @param fun
    * @param bf
    * @tparam B
    * @return
    */
  def splitBy[B](fun: A => B)(implicit bf: CanBuildFrom[T[A], A, T[A]]): Iterable[(B, T[A])] = {
    if(t.isEmpty){
      Nil
    }
    else {
      val buffer = new ListBuffer[(B, T[A])]()
      var groupBuilder = bf(t)
      var prevB: Option[B] = None
      t.foreach{
        a =>
          val b = Some(fun(a))
          if(prevB != b){
            if(prevB.isDefined){
              buffer += prevB.get -> groupBuilder.result()
            }
            prevB = b
            groupBuilder = bf(t)
          }
          groupBuilder += a
      }
      buffer += prevB.get -> groupBuilder.result()
      buffer.result()
    }
  }

}
