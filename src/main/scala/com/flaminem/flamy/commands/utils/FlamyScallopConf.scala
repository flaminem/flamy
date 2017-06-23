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

package com.flaminem.flamy.commands.utils

import org.rogach.scallop.{CliOption, LazyMap, Scallop, ScallopConf, ValueConverter}

import scala.language.existentials
import scala.reflect.runtime.universe.TypeTag

/**
  * This class overrides some methods of ScallopConf to allow a better control of how the CLI parsing works.
  */
trait FlamyScallopConf extends ScallopConf {
  import FlamyScallopConf._

  helpWidth(HELP_WIDTH)

  /**
    * Overrides ScallopConf.propsLong
    * Has exactly the same behavior except that a space must be present between
    * the option name and the arguments.
    */
  override def propsLong[A](
    name: String = "Props",
    descr: String = "",
    keyName: String = "key",
    valueName: String = "value",
    hidden: Boolean = false
  ) (implicit conv: ValueConverter[Map[String,A]]): Map[String, A] = {
    editBuilder {
      scallop =>
        scallop.copy(
          opts =
            scallop.opts :+ CustomLongNamedPropertyOption(
              name,
              descr,
              conv,
              keyName,
              valueName,
              hidden
            )
        )
    }
    val n = getName(name)
    new LazyMap({
      assertVerified
      rootConfig.builder(n)(conv.tag)
    })
  }

}

object FlamyScallopConf {

  val HELP_WIDTH = 160


  /**
    * Replaces scallop's LongNamedPropertyOption.
    * Has exactly the same behavior except that a space must be present between
    * the option name and the arguments.
    */
  case class CustomLongNamedPropertyOption(
    name: String,
    descr: String,
    converter: ValueConverter[_],
    keyName: String,
    valueName: String,
    hidden: Boolean)
    extends CliOption {

    override def longNames: List[String] = List(name)

    override def requiredShortNames: List[Char] = Nil

    override def shortNames: List[Char] = Nil

    override def isPositional: Boolean = false

    override def required: Boolean = false

    override def validator: (TypeTag[_], Any) => Boolean = (a,b) => true

    override def default: () => Option[Any] = () => Some(Map())

    override def argLine(sh: List[Char]): String = {
      "--%1$s %2$s=%3$s [%2$s=%3$s]..." format (name, keyName, valueName)
    }

    override def helpInfo(sh: List[Char]): List[(String, String, Option[String])] = {
      List((argLine(sh), descr, None))
    }
  }

}
