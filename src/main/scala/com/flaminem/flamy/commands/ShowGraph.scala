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
import com.flaminem.flamy.conf.{FlamyContext, FlamyGlobalContext, FlamyGlobalOptions}
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.exec.utils.{ReturnStatus, ReturnSuccess}
import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.core.Model
import com.flaminem.flamy.model.files.FilePath
import com.flaminem.flamy.model.names.ItemName
import org.apache.hadoop.fs.Path
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.util.Try
import scala.util.control.NonFatal

/**
  * Created by fpin on 12/17/16.
  */
class ShowGraph extends Subcommand("graph") with FlamySubcommand { options =>

  banner("Display the dependency graph of the specified items. Always show the ingoing edges.")

  //    val in: ScallopOption[Boolean] =
  //      opt(name = "in", default = Some(true), short = 'i', descr = "The graph will also display all the outgoing edges.")

  private val inWithDefs: ScallopOption[Boolean] =
    opt(name = "in-defs", default = Some(false), short = 'I', descr = "The graph will display all the parents of the selected items, and their definitions.")

  //    mutuallyExclusive(in, inWithDefs)

  private val out: ScallopOption[Boolean] =
    opt(name = "out", default = Some(false), short = 'o', descr = "The graph will also display all the children of the selected items, and their definitions.")

  //    private val outWithDefs: ScallopOption[Boolean] =
  //      opt(name = "out-defs", default = Some(false), short = '0', descr = "The graph will display all the outgoing edges.")

  /* temporary exclusion */
  mutuallyExclusive(inWithDefs, out)

  //    private val outWithDefs: ScallopOption[Boolean] =
  //      opt(name = "out-defs", default = Some(false), short = 'O', descr = "The graph will also display all the outgoing edges.")
  //
  //    mutuallyExclusive(out, outWithDefs)

  private val complete: ScallopOption[Boolean] =
    opt(name = "complete", default = Some(false), noshort = true, descr = "Build the graph with the full parser instead of the pre-parser.")

  private val schemaOnly: ScallopOption[Boolean] =
    opt(name = "schema-only", default = Some(false), noshort = true, descr = "Show only the dependencies between schemas.")

  private val skipViews: ScallopOption[Boolean] =
    opt(name = "skip-views", default = Some(false), short = 'v', descr = "Display the tables behind the views")

  private val from: ScallopOption[List[ItemName]] =
    opt[List[ItemName]](name="from", default=Some(Nil), descr="start from the given schemas/tables.", noshort=true, argName = "items")

  private val to: ScallopOption[List[ItemName]] =
    opt[List[ItemName]](name="to", default=Some(Nil), descr="stop at the given schemas/tables.", noshort=true, argName = "items")
  codependent(from,to)

  private val items: ScallopOption[List[ItemName]] =
    trailArg[List[ItemName]](default=Some(List()),required=false, descr="schema/tables to display in the graph")

  private lazy val itemArgs = ItemArgs(items(), from(), to())

  private val defaultFilenamePrefix = "graph"
  private val itemsSeparator = " "

  private def makeGraphPath(graphDir: String, items: List[ItemName]): String = {
    makeGraphPath(graphDir, ItemArgs(items, Nil, Nil))
  }

  private def makeGraphPath(graphDir: String, itemArgs: ItemArgs): String = {
    val prefix = graphDir + "/" + defaultFilenamePrefix + itemsSeparator
    val suffix: String =
      itemArgs match {
        case ItemList(itemsList) =>
          itemsList.mkString(itemsSeparator)
        case ItemRange(from, to) =>
          s"--from ${from.mkString(itemsSeparator)} --to ${to.mkString(itemsSeparator)}"
      }
    if(defaultFilenamePrefix.length + itemsSeparator.length + suffix.length > 250){
      prefix + suffix.hashCode
    }
    else {
      prefix + suffix
    }
  }


  /**
    * Try to open the graph if an opening command is found.
    */
  private def openGraph(graphPaths: String *): Unit = {
    val openCommand: String = FlamyGlobalContext.AUTO_OPEN_COMMAND.getProperty
    val multiOpen: Boolean = FlamyGlobalContext.AUTO_OPEN_MULTI.getProperty
    if(openCommand == ""){
      /* when the users sets the opening command to an empty string, we don't open the file */
    }
    else {
      /*
       * On MacOSX, we need to open all files to be able to switch between them,
       * but on Linux, we can only open one file with xdg-open, so we try to open
       * all files first, and if it fails, we only open the first file.
       */
      if(multiOpen){
        Runtime.getRuntime.exec((openCommand+:graphPaths).toArray)
      }
      else {
        Runtime.getRuntime.exec(Array(openCommand, graphPaths.head))
      }
    }
  }

  private implicit class GraphExtension(g: TableGraph) {

    def applyFilter: TableGraph = {
      val tables = g.subGraph(itemArgs).vertices
      val filter = new ItemFilter(tables, acceptIfEmpty = itemArgs.isEmpty)
      if(options.out()){
        g.filterKeepAllEdges{filter}
      }
      else if (options.inWithDefs()){
        g.filterKeepIngoingEdgesWithDefinition{filter}
      }
      else {
        g.filterKeepIngoingEdges{filter}
      }
    }

    def applySkipViews: TableGraph = {
      if(options.skipViews()){
        g.withRemovedViews(itemArgs.allItems)
      }
      else {
        g
      }
    }

  }

  // TODO: views are not shown
  private def showGraph(context: FlamyContext): Unit = {
    val model: Model =
      if(complete()){
        Model.getCompleteModel(context, Nil)
      }
      else{
        Model.getIncompleteModel(context, Nil)
      }
    val g: TableGraph = TableGraph(model).applySkipViews.applyFilter

    val graphDir: String = FlamyGlobalContext.RUN_DIR.getProperty + "/result"
    context.getLocalFileSystem.fileSystem.mkdirs(new Path(graphDir))

    val graphPath = makeGraphPath(graphDir, items())

    val lightPath = s"${graphPath}_light.png"
    val fullPath = s"$graphPath.png"

    if(schemaOnly()){
      g.export.toSchemaPng(graphPath)
      println(
        f"""graph printed at :
           |   ${FilePath(fullPath)}
           """.stripMargin
      )
      openGraph(fullPath)
    }
    else {
      g.export.toLightPng(graphPath + "_light")
      g.export.toFullPng(graphPath)
      println(
        f"""graphs printed at :
           |   ${FilePath(fullPath)}
           |   ${FilePath(lightPath)}
           """.stripMargin
      )
      openGraph(lightPath, fullPath)
    }
  }

  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
    val context = new FlamyContext(globalOptions)
    showGraph(context)
    ReturnSuccess
  }


}
