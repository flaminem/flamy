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
import com.flaminem.flamy.exec.run.GraphRunner
import com.flaminem.flamy.exec.utils.{ReturnFailure, ReturnStatus}
import com.flaminem.flamy.model.ItemArgs
import com.flaminem.flamy.model.names.ItemName
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.language.reflectiveCalls

/**
 * Created by fpin on 5/22/15.
 */
class Run extends Subcommand("run") with FlamySubcommand{

  banner("Perform a run on the specified environment")

  private val environment: ScallopOption[Environment] =
    opt(name="on", default=None, descr="Specifies environment to run on.", required=false, noshort=true)

  private val dryRun: ScallopOption[Boolean] =
    opt(name="dry", default=Some(false), descr="Perform a dry-run", noshort=true)

  validateOpt(environment, dryRun) {
    case (None,Some(false)) => Left("Please specify an environment to run on (with the --on option), or use the --dry option to perform a local dry-run")
    case _ => Right(())
  }

  private val from: ScallopOption[List[ItemName]] =
    opt[List[ItemName]](name="from", default=Some(Nil), descr="start from the given schemas/tables.", noshort=true, argName = "items")

  private val to: ScallopOption[List[ItemName]] =
    opt[List[ItemName]](name="to", default=Some(Nil), descr="stop at the given schemas/tables.", noshort=true, argName = "items")
  codependent(from,to)

  private val items: ScallopOption[List[ItemName]] =
    trailArg[List[ItemName]](default=Some(Nil),required=false)

  lazy val itemArgs = ItemArgs(items(), from(), to())

  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
    val context = new FlamyContext(globalOptions, this.environment.get)
    context.dryRun = this.dryRun()
    if (itemArgs.isEmpty) {
      System.err.println("Please specify items to run on")
      ReturnFailure
    }
    else {
      val graphRunner = GraphRunner(itemArgs, context)
      graphRunner.run()
    }
  }

}
