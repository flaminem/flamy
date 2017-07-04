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
import com.flaminem.flamy.conf.spark.ModelSparkContext
import com.flaminem.flamy.conf.{Environment, FlamyContext, FlamyGlobalOptions}
import com.flaminem.flamy.exec.FlamyRunner
import com.flaminem.flamy.exec.files.{FileRunner, ItemFileAction}
import com.flaminem.flamy.exec.hive.{HivePartitionFetcher, ModelHivePartitionFetcher}
import com.flaminem.flamy.exec.utils._
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.core.Model
import com.flaminem.flamy.model.files.FilePath
import com.flaminem.flamy.model.names.ItemName
import org.apache.spark.sql.SQLContext
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.language.reflectiveCalls

/**
 * Created by fpin on 5/22/15.
 */
class Check extends Subcommand("check") with FlamySubcommand {

  val quick = new Subcommand("quick") with FlamySubcommand {
    banner("Quick validation of hive queries. Faster than 'check long' but less exhaustive.")
    val items: ScallopOption[List[ItemName]] =
      trailArg[List[ItemName]](default=Some(List()),required=false)

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val context = new FlamyContext(globalOptions)
      Model.quickCheck(context, items())
      ReturnSuccess
    }

  }

  val long = new Subcommand("long") with FlamySubcommand {
    banner("Long validation of hive queries: performs a dry-run on all specified items against the local model.")
    val environment: ScallopOption[Environment] =
      opt(name="on", default=None, descr="Specifies environment to run on", required=false, noshort=true)
    val items: ScallopOption[List[ItemName]] =
      trailArg[List[ItemName]](default=Some(List()),required=false)

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val context = new FlamyContext(globalOptions,environment.get)
      context.dryRun = true

      /* The graph of all specified items and their dependencies */
      val baseGraph: TableGraph = TableGraph.getCompleteModelGraph(context, items(), checkNoMissingTable = false)

      /* We will only run on the specified items, and not their dependencies */
      val runGraph: TableGraph = baseGraph.subGraph(items())

      val dryRunner: FlamyRunner = FlamyRunner(context)
      println("Creating schemas and tables ...")
      try {
        dryRunner.checkAll(baseGraph)
      }
      finally{
        //TODO: For some strange reason, closing the connection here will result in ClassNotFoundErrors for udfs in the RunActions...
        //      dryRunner.close()
      }
      FlamyOutput.out.info("Running Populates ...")
      dryRunner.populateAll(runGraph.model, context)
      dryRunner.close()
      ReturnStatus(success = dryRunner.getStats.getFailCount==0)
    }

  }

  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
    subCommands match {
      case  (command: FlamySubcommand)::Nil => command.doCommand(globalOptions, Nil)
      case Nil => throw new IllegalArgumentException("A subcommand is expected")
      case _ =>
        printHelp()
        ReturnFailure
    }
  }


}
