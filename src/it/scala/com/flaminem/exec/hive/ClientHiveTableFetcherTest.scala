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

package com.flaminem.exec.hive

import com.flaminem.flamy.Launcher
import com.flaminem.flamy.conf.{Environment, FlamyContext, FlamyGlobalOptions}
import com.flaminem.flamy.exec.hive.{ClientHivePartitionFetcher, HivePartitionFetcher}
import com.flaminem.flamy.exec.utils.ReturnStatus
import com.flaminem.flamy.utils.CliUtils
import org.apache.hadoop.fs.Path
import org.scalatest._

class ClientHiveTableFetcherTest extends FreeSpec with Matchers with BeforeAndAfterEach {

  def launch(line: String): ReturnStatus = {
    val args: Array[String] = CliUtils.split(line).filter{_.nonEmpty}.toArray
    Launcher.launch(args)
  }

  val context = new FlamyContext(
    new FlamyGlobalOptions(
      conf = Map(
        "flamy.model.dir.paths" -> "src/it/resources/ClientHivePartitionFetcher"
      )
    ),
    env = Some(Environment("test"))
  )




  override def beforeEach(): Unit = {
    launch("drop tables --on test --all")
    launch("drop schemas --on test --all")
    launch("push schemas --on test")
    launch("push tables --on test")
    launch("repair tables --on test")
  }

  "ClientHivePartitionFetcher" - {

    "listTableNames" in {
      val fetcher = HivePartitionFetcher(context)
      assert(fetcher.isInstanceOf[ClientHivePartitionFetcher])
      val tables = fetcher.listTableNames
      assert(tables.size == 6)
    }

  }

}
