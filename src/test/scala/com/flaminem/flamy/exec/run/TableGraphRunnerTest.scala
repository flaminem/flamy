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

package com.flaminem.flamy.exec.run

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.conf.hive.ModelHiveContext
import com.flaminem.flamy.exec.FlamyRunner
import com.flaminem.flamy.exec.utils.Workflow.Status
import com.flaminem.flamy.exec.utils.{HasWorkflowHistory, Workflow}
import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model.ItemRange
import com.flaminem.flamy.model.names.TableName
import org.scalatest.FreeSpec

/**
  * Created by fpin on 11/10/16.
  */
class TableGraphRunnerTest extends FreeSpec {

  val context =
    new FlamyContext(
      "flamy.model.dir.paths" -> "src/test/resources/GraphRunner",
      "flamy.env.model.hive.presets.path" -> "src/test/resources/GraphRunner/PRESETS.hql"
    )
  context.dryRun = true


  "a GraphRunner should run tables in the correct order" in {
    ModelHiveContext.reset()
    val itemArgs = ItemRange(Seq("db_dest.dest"), Seq("db_dest.dest1", "db_dest.dest2"))
    val runGraph: TableGraph = TableGraph.getCompleteModelGraph(context, itemArgs, checkNoMissingTable = true).subGraph(itemArgs).withRemovedViews()
    val flamyRunner: FlamyRunner = FlamyRunner(context)
    val graphRunner = new GraphRunner(flamyRunner, context, runGraph) with HasWorkflowHistory[RunAction]

    graphRunner.run()

    val actual : Seq[(TableName, Status)] =
      graphRunner
        .getHistory
        .collect{
          case (action, Workflow.Status.RUNNING) => action.tableName -> Workflow.Status.RUNNING
          case (action, Workflow.Status.SUCCESSFUL) => action.tableName -> Workflow.Status.SUCCESSFUL
        }

    val expected =
      Seq(
        TableName("db_dest.dest") -> Workflow.Status.RUNNING,
        TableName("db_dest.dest") -> Workflow.Status.SUCCESSFUL,
        TableName("db_dest.dest2") -> Workflow.Status.RUNNING,
        TableName("db_dest.dest2") -> Workflow.Status.SUCCESSFUL,
        TableName("db_dest.dest1") -> Workflow.Status.RUNNING,
        TableName("db_dest.dest1") -> Workflow.Status.SUCCESSFUL
      )

    assert(actual === expected)
  }


  "a GraphRunner should run tables in the correct order bis" in {
    ModelHiveContext.reset()
    val itemArgs = ItemRange(Seq("db_dest2.dest"), Seq("db_dest2.dest1", "db_dest2.dest2", "db_dest2.dest3"))
    val runGraph: TableGraph = TableGraph.getCompleteModelGraph(context, itemArgs, checkNoMissingTable = true).subGraph(itemArgs).withRemovedViews()
    val flamyRunner: FlamyRunner = FlamyRunner(context)
    val graphRunner = new GraphRunner(flamyRunner, context, runGraph) with HasWorkflowHistory[RunAction]

    graphRunner.run()

    val actual : Seq[(TableName, Status)] =
      graphRunner
        .getHistory
        .collect{
          case (action, Workflow.Status.RUNNING) => action.tableName -> Workflow.Status.RUNNING
          case (action, Workflow.Status.SUCCESSFUL) => action.tableName -> Workflow.Status.SUCCESSFUL
        }

    val expected =
      Seq(
        TableName("db_dest2.dest") -> Workflow.Status.RUNNING,
        TableName("db_dest2.dest") -> Workflow.Status.SUCCESSFUL,
        TableName("db_dest2.dest2") -> Workflow.Status.RUNNING,
        TableName("db_dest2.dest2") -> Workflow.Status.SUCCESSFUL,
        TableName("db_dest2.dest3") -> Workflow.Status.RUNNING,
        TableName("db_dest2.dest3") -> Workflow.Status.SUCCESSFUL,
        TableName("db_dest2.dest1") -> Workflow.Status.RUNNING,
        TableName("db_dest2.dest1") -> Workflow.Status.SUCCESSFUL
      )

    assert(actual === expected)
  }



}
