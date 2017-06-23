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

package com.flaminem.flamy.commands

import com.flaminem.flamy.commands.utils.FlamySubcommand
import com.flaminem.flamy.conf.{FlamyContext, FlamyGlobalOptions}
import com.flaminem.flamy.exec.shell.Shell
import com.flaminem.flamy.exec.utils.{ReturnStatus, ReturnSuccess}
import org.rogach.scallop.{ScallopConf, Subcommand}

import scala.language.reflectiveCalls

/**
 * Created by fpin on 5/22/15.
 */
class Shell extends Subcommand("shell") with FlamySubcommand{

  banner("Launch a shell where every flamy command is available, and much more responsive!!!")

  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
    val context = new FlamyContext(globalOptions)
    assert(subCommands.isEmpty)
    new com.flaminem.flamy.exec.shell.Shell().mainLoop(context)
    ReturnSuccess
  }

}

