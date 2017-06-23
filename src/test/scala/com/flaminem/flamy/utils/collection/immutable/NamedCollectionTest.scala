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

import com.flaminem.flamy.utils.Named
import org.scalatest.FreeSpec

/**
  * Created by fpin on 10/13/16.
  */
class NamedCollectionTest extends FreeSpec {

  case class Person(name: String) extends Named {
    override def getName: String = name
  }

  "NamedCollection" - {

    "should be sorted" in {
      val c: NamedCollection[Person] = NamedCollection[Person](Person("Joe"), Person("Jack"), Person("William"), Person("Averell"))
      assert(c.toSeq === Seq(Person("Averell"), Person("Jack"), Person("Joe"), Person("William")))
    }

    "+ should work" in {
      val c: NamedCollection[Person] = NamedCollection[Person]()
      val c2: NamedCollection[Person] = c + Person("john")
      assert(c2.size === 1)
    }

    "++ should work" in {
      val c: NamedCollection[Person] = NamedCollection[Person]()
      val c2: NamedCollection[Person] = c ++ Seq(Person("Joe"), Person("Jack"), Person("William"), Person("Averell"))
      assert(c2.size === 4)
    }

    "- should work" in {
      val c: NamedCollection[Person] = NamedCollection[Person](Person("Joe"), Person("Jack"), Person("William"), Person("Averell"))
      val c2: NamedCollection[Person] = c - Person("Joe")
      assert(c2.toSeq === Seq(Person("Averell"), Person("Jack"), Person("William")))
    }

    "map should work" in {
      val c: NamedCollection[Person] = NamedCollection[Person](Person("Joe"), Person("Jack"))
      def daltonify(p:Person): Person =  p match {case Person(name) => Person(s"$name Dalton")}
      val c2: NamedCollection[Person] = c.map{daltonify}
      assert(c2.toSeq === Seq(Person("Jack Dalton"), Person("Joe Dalton")))
    }

  }



}
