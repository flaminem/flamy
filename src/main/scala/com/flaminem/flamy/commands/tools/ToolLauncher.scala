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

package com.flaminem.flamy.commands.tools

import com.flaminem.flamy.Launcher.Options
import com.flaminem.flamy.exec.utils.ReturnStatus
import com.flaminem.flamy.exec.utils.io.FlamyOutput

import scala.language.reflectiveCalls

/**
  * This is an alternate launcher that expose other commands mostly useful for the developpers,
  * that we do not wish to expose to end-users.
  */
object ToolLauncher {

  com.flaminem.flamy.Launcher.newLauncher = {
    args: Seq[String] => new ToolLauncher(args)
  }

  /* For some reason, overriding Scallop's commands does not work */
  class Commands(args: Seq[String]) extends Options(args) {
    import com.flaminem.flamy.commands

    val export = new commands.tools.Export
  }

  /*
   * Only this method is allowed to call System.exit
   */
  def main(args: Array[String]): Unit = {
    FlamyOutput.init()
    val returnStatus =
      try {
        new ToolLauncher(args, withShell = true).launch()
      }
      finally{
        FlamyOutput.shutdown()
      }
    System.exit(returnStatus.exitCode)
  }

  def launch(args: Seq[String]): ReturnStatus = {
    new ToolLauncher(args).launch()
  }

}

class ToolLauncher(args: Seq[String], withShell: Boolean = false) extends com.flaminem.flamy.Launcher(args) {

  import ToolLauncher._

  override lazy val opts = new Commands(args)

}
