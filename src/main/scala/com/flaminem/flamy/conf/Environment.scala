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

package com.flaminem.flamy.conf

import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.model.names.TableName.fromArg
import org.rogach.scallop.ValueConverter
import scala.reflect.runtime.universe._

/**
  * Created by fpin on 5/15/17.
  */
case class Environment(name: String) {

  override def toString: String = {
    name
  }

}


object Environment {

  implicit val scallopConverter: ValueConverter[Environment] = {
    new ValueConverter[Environment] {
      override def parse(args: List[(String, List[String])]): Either[String, Option[Environment]] = {
        args match {
          case (_, a::Nil)::Nil => Right(Some(Environment(a)))
          case Nil => Right(None)
          case _ => Left("")
        }
      }
      override val tag: TypeTag[Environment] = typeTag[Environment]
      override val argType = org.rogach.scallop.ArgType.SINGLE
    }
  }


}