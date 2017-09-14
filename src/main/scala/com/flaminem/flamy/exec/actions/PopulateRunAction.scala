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

package com.flaminem.flamy.exec.actions

import com.flaminem.flamy.conf.{FlamyContext, FlamyGlobalContext}
import com.flaminem.flamy.exec.FlamyRunner
import com.flaminem.flamy.model.{Inputs, PopulateInfo}
import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.parsing.hive.QueryUtils
import com.flaminem.flamy.parsing.hive.run.RunActionParser
import com.flaminem.flamy.utils.AutoClose

class PopulateRunAction (
  val tableName: TableName,
  val populateInfo: PopulateInfo,
  context: FlamyContext
) extends RunAction with PopulateAction {

  override def hasDynamicPartitions: Boolean = populateInfo.hasDynamicPartitions

  /**
    * Stores the input information: number of partition and number of tables read
    */
  private var inputs: Option[Inputs] = None

  private def inputString: Option[String] = {
    inputs.flatMap{
      case Inputs.NoInputs => None
      case Inputs(input_partitions, input_tables) =>
        Some(
          s"reading from ${input_partitions.size} partitions in ${input_tables.size} tables"
        )
    }
  }

  private def computeInputs(queries: Seq[String], runner: FlamyRunner): Unit = {
    if(FlamyGlobalContext.REGEN_SHOW_INPUTS.getProperty) {
      inputs = Some(queries.filterNot{QueryUtils.isCommand}.flatMap{runner.getInputsFromText}.foldLeft[Inputs](Inputs.NoInputs){ _ + _ } )
    }
  }

  override def interrupt(): Unit = {
    currentRunner.foreach{_.interrupt()}
    super.interrupt()
  }

  @volatile
  private var currentRunner: Option[FlamyRunner] = None

  override def run(): Unit = {
    val queries: Seq[String] =
      new RunActionParser().parseText(populateInfo.tableFile.text, context.getVariables, isView = false)(context)
    val startTime = System.currentTimeMillis()
    for {
      runner <- AutoClose(FlamyRunner(context), {r: FlamyRunner => r.close() ; currentRunner = None})
    } {
      currentRunner = Some(runner)
      computeInputs(queries, runner)
      _message = inputString
      queries.foreach{
        query =>
          val jobTitle: String = s"POPULATE $name"
          runner.runText(query, Some(jobTitle))
      }
      val endTime = System.currentTimeMillis()
      _message = Some(s"${(endTime-startTime)/1000} seconds")
    }
  }

  @volatile var _message: Option[String] = None

  override def message: Option[String] = {
    _message
  }

  override val name: String = populateInfo.title

  override val logPath: String = populateInfo.tableFile.getRelativePath
}
