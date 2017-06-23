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
import com.flaminem.flamy.exec.hive.HivePartitionFetcher
import com.flaminem.flamy.exec.utils.{ReturnStatus, ReturnSuccess}
import com.flaminem.flamy.model.ItemFilter
import com.flaminem.flamy.model.names.ItemName
import com.flaminem.flamy.utils.AutoClose
import com.flaminem.flamy.utils.time.TimeUtils
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.language.reflectiveCalls

/**
  * Created by fpin on 3/9/16.
  */
class GatherInfo extends Subcommand("gather-info") with FlamySubcommand {

  banner("Gather all partitioning information on specified items (everything if no argument is given) and output this as csv on stdout.")

  val environment: ScallopOption[Environment] =
    opt(name = "on", descr = "Specifies environment to run on", required = true, noshort = true)

  val items: ScallopOption[List[ItemName]] =
    trailArg[List[ItemName]](default = Some(List()), required = false)

  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
    val context = new FlamyContext(globalOptions, this.environment.get)
    for{
      fetcher: HivePartitionFetcher <- AutoClose(HivePartitionFetcher(context))
    } {
      val itemFilter = new ItemFilter(this.items(), true)
      for {
        tpInfo <- fetcher.listTableNames.filter{itemFilter}.map{fetcher.getTablePartitioningInfo}
        partition <- tpInfo.sortedTablePartitions
      } {
        println(
          Seq(
            tpInfo.tableName.schemaName,
            tpInfo.tableName.name,
            partition.partitionName,
            partition.getFileSize.getOrElse("\\N"),
            partition.getModificationTime(context, refresh = false).map {
              TimeUtils.timestampToUniversalTime
            }.getOrElse("\\N")
          ).mkString("\t")
        )
      }
    }
    ReturnSuccess
  }

}
