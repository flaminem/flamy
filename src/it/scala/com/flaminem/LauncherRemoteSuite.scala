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

package com.flaminem

import com.flaminem.flamy.Launcher
import com.flaminem.flamy.exec.utils.ReturnStatus
import com.flaminem.flamy.utils.CliUtils
import org.scalatest._

class LauncherRemoteSuite extends FreeSpec with BeforeAndAfterAll {

  def launch(line: String): ReturnStatus = {
    val args: Array[String] = CliUtils.split(line).filter{_.nonEmpty}.toArray
    Launcher.launch(args)
  }

  override def beforeAll(): Unit = {
    launch("drop tables --on test --all")
    launch("drop schemas --on test --all")
    launch("push schemas --on test")
    launch("push tables --on test")
    launch("repair tables --on test")
  }

  "show schemas --on test" in {
    assert(launch("show schemas --on test").isSuccess)
  }
  "show tables --on test" in {
    assert(launch("show tables --on test").isSuccess)
  }
  "show partitions --on test" in {
    assert(launch("show partitions --on test").isSuccess)
  }


  "describe schemas --on test" in {
    assert(launch("describe schemas --on test").isSuccess)
  }
  "describe tables --on test" in {
    assert(launch("describe tables --on test").isSuccess)
  }
  "describe partitions --on test" in {
    assert(launch("describe partitions --on test").isSuccess)
  }


  "check long --on test" in {
    assert(launch("check long --on test").isSuccess)
  }


  "run --on test" in {
    assert(launch("run --on test db_dest").isSuccess)
  }
  "run --on test --from --to" in {
    assert(launch("run --on test --from db_dest --to db_dest").isSuccess)
  }

}
