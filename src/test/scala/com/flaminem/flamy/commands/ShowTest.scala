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

import com.flaminem.flamy.Launcher
import com.flaminem.flamy.exec.utils.ReturnSuccess
import org.scalatest.FreeSpec

import scala.language.implicitConversions

/**
  * Created by fpin on 2/13/17.
  */
class ShowTest extends FreeSpec {

  implicit def stringToSeq(s: String): Seq[String] = s.split("\\s+")

  "show tables should work" in {
    val exitCode = Launcher.launch("--conf flamy.model.dir.paths=src/test/resources/test show tables")
    assert(exitCode === ReturnSuccess)
  }

  "show tables should work when there is a cycle in the graph" in {
    val exitCode = Launcher.launch("--conf flamy.model.dir.paths=src/test/resources/Graph show tables")
    assert(exitCode === ReturnSuccess)
  }

}
