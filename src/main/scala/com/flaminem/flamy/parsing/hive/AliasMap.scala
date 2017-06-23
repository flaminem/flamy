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

package com.flaminem.flamy.parsing.hive

import com.flaminem.flamy.parsing.model.TableDependency
import com.flaminem.flamy.utils.collection.immutable.UniqueSeqMapLike

import scala.collection.immutable.TreeMap

/**
  * A collection where we store the tabledependencies indexed with their aliases.
  * It has the following properties:
  * - The aliases are case-insensitive.
  * - Inserting the same alias twice will throw a UnicityConstraintException.
  * - Iterating over the keys, values, or key-value pairs will preserve the insertion order.
  *
  * @param entries
  * @param map
  */
class AliasMap private (
  override val entries: Seq[(String, TableDependency)] = Seq() ,
  override val map: Map[String, TableDependency] = TreeMap[String, TableDependency]()(scala.math.Ordering.comparatorToOrdering(String.CASE_INSENSITIVE_ORDER))
) extends UniqueSeqMapLike[String, TableDependency, AliasMap]
{

  /**
    * Returns a new instance of the collection, containing the specified map
    */
  override def copy(entries: Seq[(String, TableDependency)], map: Map[String, TableDependency]): AliasMap = {
    new AliasMap(entries, map)
  }

  def aliases: Iterable[String] = {
    keys
  }

  def tables: Iterable[TableDependency] = {
    values
  }

}


object AliasMap {

  def apply(pairs: (String, TableDependency)*): AliasMap = {
    new AliasMap() ++ pairs
  }

  def apply(pairs: Iterable[(String, TableDependency)]): AliasMap = {
    new AliasMap() ++ pairs
  }

}

