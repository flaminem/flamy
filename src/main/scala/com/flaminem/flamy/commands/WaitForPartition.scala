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
import com.flaminem.flamy.exec.hive.PartitionWaiter
import com.flaminem.flamy.exec.utils.ReturnStatus
import com.flaminem.flamy.model.names.ItemName
import com.flaminem.flamy.utils.AutoClose
import com.flaminem.flamy.utils.time.TimeUtils
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.language.reflectiveCalls

/**
  * Created by fpin on 3/9/16.
  */
class WaitForPartition extends Subcommand("wait-for-partition") with FlamySubcommand{

  banner("Wait for a partition to be created.")

  val environment: ScallopOption[Environment] =
    opt(name = "on", descr = "Specifies environment to run on", required = true, noshort = true)
  val timeout: ScallopOption[Long] =
    opt(
      name = "timeout",
      descr = "Number of seconds after which flamy will fail if the partitions still does not exist",
      default = Some(12 * 3600),
      noshort = true
    )
  val after: ScallopOption[String] =
    opt(
      name = "after",
      argName = "yyyy-MM-dd HH:mm:ss",
      descr = """Wait for the partition to be created or refreshed after this time. Expected format is "yyyy-MM-dd HH:mm:ss"""",
      default = None,
      noshort = true
    )
  val retryInterval: ScallopOption[Long] =
    opt(
      name = "retry-interval",
      argName = "INTERVAL",
      descr = "When a partition is not found, retry after INTERVAL seconds",
      default = Some(60),
      noshort = true
    )
  val items: ScallopOption[List[ItemName]] =
    trailArg[List[ItemName]](required = true)

  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
    val context = new FlamyContext(globalOptions, environment.get)
    val waiter = new PartitionWaiter(context)
    for{
      waiter: PartitionWaiter <- AutoClose(new PartitionWaiter(context))
    } yield {
      waiter.waitForPartition(items(), timeout(), after.get.map{TimeUtils.universalTimeToTimeStamp}, retryInterval())
    }
  }

}
