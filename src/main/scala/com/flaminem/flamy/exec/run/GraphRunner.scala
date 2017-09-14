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
import com.flaminem.flamy.exec.FlamyRunner
import com.flaminem.flamy.exec.actions.{PopulateRunAction, RunAction, SkipRunAction}
import com.flaminem.flamy.exec.utils._
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.graph._
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.model.names.TableName

class GraphRunner (
  val flamyRunner: FlamyRunner,
  override val context: FlamyContext,
  /** Contains the sub-graph of all the tables that requires to be run. */
  val runGraph: TableGraph
)(implicit override val actionOrdering: Ordering[RunAction]) extends ParallelActionRunner[RunAction](context)(actionOrdering) {

  /**
    * Build the action to be run for the given table.
    * This method can be overridden to change the behavior of the runner.
    * @param table
    * @return
    */
  protected def buildActionForTable(table: TableName): Seq[RunAction] = {
    runGraph.model.getTable(table).get.populateInfos.map {
      populate => new PopulateRunAction(table, populate, context)
    }
  }

  private def safeBuildActionForTable(table: TableName): Seq[RunAction] = {
    val actions = buildActionForTable(table)
    if(actions.isEmpty){
      new SkipRunAction(table) :: Nil
    }
    else{
      actions
    }
  }

  final override def getNextActions(completedAction: RunAction): Seq[RunAction] = {
    val tableName = completedAction.tableName
    if(isTableDone(tableName)){
      makeChildrenReady(tableName)
    }
    else{
      Nil
    }
  }

  /**
    * Returns true if the set of all the actions in the workflow associated with this table
    * is not empty, and if they are all successful or skipped.
    * @param tableName
    */
   private def isTableDone(tableName: TableName): Boolean = {
    val statuses =
      getAllJobStatus
        .filterKeys{_.tableName == tableName}
        .values
    statuses.nonEmpty && statuses.forall{s => s == Workflow.Status.SUCCESSFUL || s == Workflow.Status.SKIPPED}
  }

  /**
    * Returns true if the workflow contains any acion related to this table.
    * @param tableName
    * @return
    */
  private def isTableInWorkflow(tableName: TableName): Boolean = {
    getAllJobStatus
      .filterKeys{_.tableName == tableName}
      .nonEmpty
  }

  private def makeChildrenReady(table: TableName): Seq[RunAction] = {
    for {
      child: TableName <- runGraph.getChildren(table)
      /* If all parents in the workflow are done */
      if runGraph.getParents(child).forall{isTableDone}
      /*
        If the child is not already in the workflow
        TODO: Not sure if this is really necessary
      */
      if !isTableInWorkflow(child)
      /* For every populate */
      action <- safeBuildActionForTable(child)
    } yield {
      action
    }
  }

  private def checkConfig() = {
    if (context.HIVE_PRESETS_PATH.getProperty.isEmpty && !context.dryRun) {
      FlamyOutput.out.warn("Hive presets path is not defined, missing property : " + context.HIVE_PRESETS_PATH.varName)
    }
  }

  private def getStartActions: Seq[RunAction] = {
    for {
      table <- runGraph.findRoots()
      action <- safeBuildActionForTable(table)
    } yield {
      action
    }
  }

  final def run(): ReturnStatus = {
    checkConfig()
    prepareEnv()
    run(getStartActions)
  }


  @throws(classOf[FlamyException])
  private def prepareEnv() = {
    System.out.println("Preparing environment ... ")
    try {
      flamyRunner.checkAll(runGraph.baseGraph)
    }
    finally {
      //TODO: For some strange reason, closing the connection here will result in ClassNotFoundErrors for udfs in the RunActions...
//      flamyRunner.close()
    }
    System.out.println("... environment prepared.")
  }

}

object GraphRunner {

  def apply(itemArgs: ItemArgs, context: FlamyContext): GraphRunner = {
    val runGraph: TableGraph =
      TableGraph
        .getCompleteModelGraph(context, itemArgs, checkNoMissingTable = true)
        .subGraph(itemArgs)
        .withRemovedViews()
    val flamyRunner: FlamyRunner = FlamyRunner(context)
    /* We activate the PopulateConcurrencySafety only if hive's concurrency is enabled */
    flamyRunner.getConfiguration("hive.support.concurrency") match {
      case Some("true") =>
        new GraphRunner(flamyRunner, context, runGraph) with PopulateConcurrencySafety[RunAction] with ActionLogging[RunAction]
      case _ =>
        new GraphRunner(flamyRunner, context, runGraph) with ActionLogging[RunAction]
    }
  }

}