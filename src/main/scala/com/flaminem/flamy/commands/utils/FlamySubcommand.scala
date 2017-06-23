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

import com.flaminem.flamy.conf.FlamyGlobalOptions
import com.flaminem.flamy.exec.files.FileRunner
import com.flaminem.flamy.exec.utils.ReturnStatus
import org.rogach.scallop.{ScallopConf, Subcommand}

import scala.language.reflectiveCalls

/**
  * Extends scallop's Subcommand by overriding some methods to allow a better control of how the CLI parsing works.
  */
trait FlamySubcommand extends Subcommand with FlamyScallopConf{

  var stats: Option[FileRunner#Stats] = None

  def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus

  override def printHelp(): Unit = {
    ScallopUtils.printHelp(this.builder, this.commandName)
  }

  override def toString: String = this.commandName

}
