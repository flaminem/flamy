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
import com.flaminem.flamy.conf.{Environment, FlamyContext, FlamyGlobalOptions}
import com.flaminem.flamy.exec.FlamyRunner
import com.flaminem.flamy.exec.utils.{ReturnStatus, ReturnSuccess}
import com.flaminem.flamy.model.files.FileIndex
import com.flaminem.flamy.model.names.ItemName
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.language.reflectiveCalls

/**
 * Created by fpin on 5/22/15.
 */
class Test extends Subcommand("test") with FlamySubcommand{

  banner("Execute all TEST.hql queries for all specified items, and assert that the result is 0.")

  private val environment: ScallopOption[Environment] =
    opt(name="on", default=None, descr="Specifies environment to run on", required=true, noshort=true)

  private val dryRun: ScallopOption[Boolean] =
    opt(name="dry", default=Some(false), descr="Perform a dry-run", noshort=true)

  private val items: ScallopOption[List[ItemName]] =
    trailArg[List[ItemName]](default=Some(List()),required=false)



  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
    val context = new FlamyContext(globalOptions, environment.get)
    context.dryRun = dryRun()

    val flamyRunner: FlamyRunner = FlamyRunner(context)

    val fileIndex = context.getFileIndex.filter(items())

    flamyRunner.testAll(fileIndex, context)

    ReturnSuccess
  }


}
