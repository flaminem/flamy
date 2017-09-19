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

class LauncherLocalSuite extends FreeSpec {

  def launch(line: String): ReturnStatus = {
    val args: Array[String] = CliUtils.split(line).filter{_.nonEmpty}.toArray
    Launcher.launch(args)
  }

  "show tables" in {
    assert(launch("show tables").isSuccess)
  }
  "show select" in {
    assert(launch("show select db_dest.dest1").isSuccess)
  }
  "show graph" ignore {
    assert(launch("show graph").isSuccess)
  }


  "check quick" in {
    assert(launch("check quick").isSuccess)
  }
  "check long" in {
    assert(launch("check long").isSuccess)
  }


  "run --dry" in {
    assert(launch("run --dry db_dest db_source").isSuccess)
  }
  "run --dry --from --to" in {
    assert(launch("run --on test --from db_dest --to db_dest").isSuccess)
  }

}
