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

package com.flaminem.flamy.model

import org.json4s.{DefaultFormats, _}
import org.json4s.native.JsonMethods._

/**
  * Created by fpin on 1/6/17.
  */
case class Inputs(input_partitions: Set[InputPartition], input_tables: Set[InputTable]) {

  def +(that: Inputs): Inputs = {
    new Inputs(this.input_partitions.union(that.input_partitions), this.input_tables.union(that.input_tables))
  }

}

case class InputPartition(partitionName: String)

case class InputTable(tablename: String)

object Inputs {

  /**
    * Read a json string and extract the specified class out of it.
    * Only the part of the json specified by the subPath will be read.
    * <h3> Mapping: </h3>
    * The field names of the case class should correspond to the one in the json string.
    * <p>
    * The case class may contain optional fields, either by using Option[T], or by using default values.
    * Optional fields do not required to be defined in the json. Other field must be defined.
    * <p>
    * "snake_case" for scala field names is exceptionally allowed by best practices, in order to keep the conf mapping simple.
    * <p>
    * Classes that can be automagically mapped to a json include :
    * - All primitive types, including BigInt and Symbol
    * - List, Seq, Array, Set and Map (note, keys of the Map must be strings: Map[String, _])
    * - scala.Option
    * - Any case class build with the above elements (include other case classes)
    *
    * @see This uses the json4s library, you can get more details here: http://json4s.org/
    * @param jsonData the json string from which the conf should be read
    * @param subPath  the path of the conf
    * @tparam T       the type of the case class to which the json conf should be mapped
    * @return
    */
  def readFromString[T: Manifest](jsonData: String, subPath: String*) (implicit formats: Formats = DefaultFormats): T = {
    val fullAST = parse(jsonData)
    val jsonAST = subPath.foldLeft(fullAST){case (ast, subField) => ast\subField}
    jsonAST.extract[T]
  }

  def fromJson(json: String): Inputs = {
    readFromString[Inputs](json)
  }

  def noInputs: Inputs = {
    Inputs(Set(), Set())
  }

}