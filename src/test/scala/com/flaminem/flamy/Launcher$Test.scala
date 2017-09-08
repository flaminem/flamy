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

package com.flaminem.flamy

import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.exec.utils.{ReturnFailure, ReturnStatus, ReturnSuccess}
import com.flaminem.flamy.utils.CliUtils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FreeSpec, Matchers}

import scala.language.implicitConversions

/**
 * Created by fpin on 10/26/14.
 */
class Launcher$Test extends FreeSpec with Matchers with BeforeAndAfterEach {

  implicit def stringToArray(s: String): Array[String] = s.split("\\s+")

//  override def beforeEach(): Unit = {
//    super.beforeEach()
//    ModelHiveContext.reset()
//  }

  def launch(line: String): ReturnStatus = {
    val args: Array[String] = CliUtils.split(line).filter{_.nonEmpty}.toArray
    Launcher.launch(args)
  }

  val testConf: String =
    """--conf flamy.model.dir.paths=src/test/resources/test
      |--conf flamy.variables.path=src/test/resources/test/VARIABLES.properties
      |--conf flamy.env.model.hive.presets.path=src/test/resources/test/PRESETS.hql
      |""".stripMargin

  "a Launcher" - {

    "with no project" - {
      "should display help and list available projects" in {
        val exitCode = launch("")
        exitCode should equal (ReturnFailure)
      }
    }

    "when option --help is called" - {
      "should display help" in {
        val exitCode = launch("--help")
        exitCode should equal (ReturnSuccess)
      }
    }

    "when show tables --help is called" - {
      "should display help" in {
        val exitCode = launch("show tables --help")
        exitCode should equal (ReturnSuccess)
      }
    }

    "with a project" - {
      "should print the help" in {
        val exitCode = launch(testConf)
        exitCode should equal (ReturnFailure)
      }

      "when asked to run a quick-check" - {
        "should run a quick-check" in {
          val exitCode = launch(testConf + "check quick")
          exitCode should equal (ReturnSuccess)
        }
      }

      "when asked to run a long-check" - {
        "should run a long-check" in {
          val exitCode = launch(testConf + "check long")
          exitCode should equal (ReturnSuccess)
        }
      }

      "when asked to run a long-check on a project with errors" - {
        "should run a long-check and fail" in {
          val exitCode = launch("--conf flamy.model.dir.paths=src/test/resources/test_failure check long")
          exitCode should equal (ReturnFailure)
        }
      }

      "when asked to perform a dry-run" - {
        "should perform a dry-run" in {
          val exitCode =
            launch(
              """--conf flamy.model.dir.paths=src/test/resources/GraphRunner
                |--conf flamy.env.model.hive.presets.path=${flamy.model.dir.paths}/PRESETS.hql
                |run --dry db_dest""".stripMargin)
          exitCode should equal (ReturnSuccess)
        }
      }

      "when asked to show config" - {
        "should show the config" in {
          val exitCode = launch("--conf flamy.model.dir.paths=src/test/resources/GraphRunner show conf")
          exitCode should equal (ReturnSuccess)
        }
      }

      "when asked to show tables" - {
        "should show tables" in {
          val exitCode = launch("--conf flamy.model.dir.paths=src/test/resources/test show tables")
          exitCode should equal (ReturnSuccess)
        }
      }

      "show graph" - {
        "should work" in {
          val exitCode =
            launch(
              """--conf flamy.model.dir.paths=src/test/resources/Graph
                |--conf flamy.auto.open.command=''
                |show graph
                |""".stripMargin
            )
          /* On the CI server, dot is not installed, so the command fails */
//          exitCode should equal (ReturnSuccess)
        }
      }

      "show graph --schema-only" - {
        "should work" in {
          val exitCode =
            launch(
              """--conf flamy.model.dir.paths=src/test/resources/Graph
                |--conf flamy.auto.open.command=''
                |show graph --schema-only
                |""".stripMargin
            )
          /* On the CI server, dot is not installed, so the command fails */
//          exitCode should equal (ReturnSuccess)
        }
      }
    }
  }
}
