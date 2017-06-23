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

import com.flaminem.flamy.conf.{Flamy, FlamyGlobalContext}
import org.rogach.scallop.Scallop

/**
  * Contains a few utility functions to help using Scallop
  */
object ScallopUtils {

  /**
    * Recursively print the subCommands and their description when they have no subcommand themselves
    * @param builder a scallop builder
    * @param recursionLevel the recursion level, used for indentation
    */
  def printSubCommands(builder: Scallop, recursionLevel: Int = 1): Unit = {
    builder
      .subbuilders
      .sortBy{_._1}
      .foreach{
        case (name, sub) if sub.shortSubcommandsHelp =>
          ()
        case (name, sub) =>
          if (sub.subbuilders.isEmpty) {
            println("  " * recursionLevel + name + " " * (26 - name.length - recursionLevel * 2) + sub.bann.getOrElse(""))
          }
          else {
            println("  " * recursionLevel + name)
            printSubCommands(sub, recursionLevel + 1)
          }
          if (recursionLevel == 1) {
            println
          }
      }
  }

  def printHelp(builder: Scallop, commandName: String): Unit = {
    /* Scallop separates the command names with \0 */
    val fullCommand = commandName.split("\u0000")
    if (builder.subbuilders.nonEmpty) {
      println(fullCommand.mkString(s"Usage: ${Flamy.name} ", " ", " <SUB_COMMAND>"))
      println("Subcommands:")
      ScallopUtils.printSubCommands(builder)
    }
    else {
      println(fullCommand.mkString(s"Usage: ${Flamy.name} ", " ", " : "))
      builder.printHelp()
    }
  }

}
