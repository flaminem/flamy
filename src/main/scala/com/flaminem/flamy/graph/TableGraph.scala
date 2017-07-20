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

package com.flaminem.flamy.graph

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.core.{IncompleteModel, Model}
import com.flaminem.flamy.model.files.{FileIndex, FileType, TableFile}
import com.flaminem.flamy.model.names.{ItemName, TableName}

import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.{Graph, GraphTraversal}

// or scalax.collection.mutable.Graph

/**
 * Created by fpin on 1/2/15.
 */
class TableGraph private(
  val model: Model,
  val graph: Graph[TableName, DiEdge],
  val _baseGraph: Option[TableGraph] = None
) {
  import TableGraph._

  def baseGraph: TableGraph = _baseGraph.getOrElse(this)

  lazy val vertices: Seq[TableName] = {
    graph.getNodeSeq
  }

  lazy val edges: Seq[(TableName, TableName)] = {
    graph.edges.toSeq.map{e => (e.source.value, e.target.value)}
  }

  /**
    * @return the topologically sorted sequence of all the vertices in the graph.
    *         Vertices that are part of a loop are not returned.
    */
  def getTopologicallySortedVertices: Seq[TableName] = {

    def recAux(current: Seq[TableName], seenSet: Set[TableName], sortedRes: Seq[TableName]): Seq[TableName] = {
      val newSeenSet = seenSet++current
      val newSortedRes = sortedRes++current
      def hasAllParentsBeenSeen(v: TableName) = getParents(v).forall{newSeenSet.contains}
      current.flatMap{getChildren}.distinct.filter{hasAllParentsBeenSeen} match {
        case Nil => newSortedRes
        case children => recAux(children,newSeenSet,newSortedRes)
      }
    }

    val roots = findRoots()
    recAux(roots, Set(), Nil)
  }

  /**
    * Return the sequence of all views in the graph, topologically sorted.
    * That is, all parents appear before their children in the sequence.
    * This function also handles views that are in a table dependency loop.
    * If there is a view dependency loop, however, we throw an exception.
    *
    * @throws TableGraphException
    * @return the topologically sorted sequence of all views.
    */
  @throws[TableGraphException]
  private[graph] def getTopologicallySortedViews: Seq[TableName] = {
    val topologicallySortedViews = this.getTopologicallySortedVertices.filter {isView}

    /* The set of all the views that are in a table dependency loop */
    val viewsInLoops: Set[TableName] = this.vertices.filter {isView}.toSet -- topologicallySortedViews

    /* The subgraph made with these views */
    val subG = this.filter{t => viewsInLoops.exists{_ == t}}

    /* We make a second topological sort on these views */
    val topologicallySortedLoopViews = subG.getTopologicallySortedVertices

    /* If we find another loop, it means that we have a loop of views, which is forbidden. */
    val loopOfViews = subG.vertices.toSet -- topologicallySortedLoopViews
    if (loopOfViews.nonEmpty) {
      throw new TableGraphException(s"A loop of views was detected: $loopOfViews")
    }

    topologicallySortedViews ++ topologicallySortedLoopViews
  }

  def getTopologicallySortedViewFiles: Seq[TableFile] = {
    for {
      tableName <- getTopologicallySortedViews
      itemFiles = this.model.fileIndex.getTableFiles(tableName.fullName)
      file <- itemFiles.getFilesOfType(FileType.VIEW)
    } yield {
      file.asInstanceOf[TableFile]
    }
  }

  /**
    *
    * @param fileType
    * @return the topologically sorted sequence of all the vertices of type <code>fileType</code>
    */
  def getTableFilesOfType(fileType: FileType): Seq[TableFile] = {
    for {
      tableName <- this.vertices
      itemFiles = this.model.fileIndex.getTableFiles(tableName.fullName)
      file <- itemFiles.getFilesOfType(fileType)
    } yield {
      file.asInstanceOf[TableFile]
    }
  }

  def findRoots(): Seq[TableName] = {
    graph.filter{graph.having(node={n => n.diPredecessors.isEmpty})}.getNodeSeq
  }

  def getChildren(vertex: TableName): Seq[TableName] = {
    graph.getChildren(vertex)
  }

  def getChildren(vertices: Seq[TableName]): Seq[TableName] = {
    vertices.flatMap{getChildren}.distinct
  }

  def getParents(vertex: TableName): Seq[TableName] = {
    graph.getParents(vertex)
  }

  /**
    * Gives the list of parents of the specified vertices. If a parent is already in the argument list, they are not returned in the parent list.
    *
    * @param vertices
    * @return
    */
  def getParents(vertices: Seq[TableName]): Seq[TableName] = {
    vertices.flatMap{getParents}.distinct.diff(vertices)
  }

  def getAllAscendants(tableName: TableName, stopAt: Set[ItemName] = Set()): Seq[TableName] = {
    assert(graph.contains(TableName(tableName.fullName)),s"$tableName was not found in the graph. Please report a bug.")
    graph.get(tableName)
      .innerNodeTraverser
      .withDirection(GraphTraversal.Predecessors)
      .toNodeSeq
  }

  /**
    * @param tableName
    * @param stopAt stop the traversal when we meet one of these nodes (the node is included but not its children)
    * @return all the descendants of the table
    */
  def getAllDescendants(tableName: TableName, stopAt: Set[ItemName] = Set()): Seq[TableName] = {
    assert(graph.contains(TableName(tableName.fullName)),s"$tableName was not found in the graph. Please report a bug.")

    graph.get(tableName)
      .innerNodeTraverser
      .withDirection(GraphTraversal.Successors)
      .toNodeSeq
  }

  /**
   * Apply a filter to the graph.
   * The ingoing edges are kept, however the information associated to their source nodes is lost.
    *
    * @param predicate the filter predicate
   * @return
   */
  def filterKeepIngoingEdges(predicate: (ItemName)=>Boolean): TableGraph = {
    new TableGraph(
      model.filter{predicate},
      graph.filter{graph.having(node={n =>predicate(n)||n.diSuccessors.exists{predicate(_)}})},
      Some(baseGraph)
    )
  }

  /**
    * Apply a filter to the graph.
    * The ingoing edges are kept with their associated information .
    *
    * @param predicate the filter predicate
    * @return
    */
  def filterKeepIngoingEdgesWithDefinition(predicate: (ItemName)=>Boolean): TableGraph = {
    val newGraph = graph.filter{graph.having(node={n =>predicate(n)||n.diSuccessors.exists{predicate(_)}})}
    new TableGraph(
      model.filter(newGraph.getNodeSeq),
      newGraph,
      Some(baseGraph)
    )
  }

  /**
   * Apply a filter to the graph.
   * Both ingoing and outgoing edges are kept, however the information associated to their source (resp. destination) nodes is lost.
    *
    * @param predicate the filter predicate
   * @return
   */
  def filterKeepAllEdges(predicate: (ItemName)=>Boolean): TableGraph = {
    new TableGraph(
      model.filter{predicate},
      graph.filter{graph.having(node={n => predicate(n) || n.diSuccessors.exists{predicate(_)} || n.diPredecessors.exists{predicate(_)} })},
      Some(baseGraph)
    )
  }

  def filter(predicate: (ItemName)=>Boolean): TableGraph = {
    new TableGraph(
      model.filter{predicate},
      graph.filter{graph.having(node={n => predicate(n)})},
      Some(baseGraph)
    )
  }

  def subGraph(from: Seq[ItemName], to: Seq[ItemName]): TableGraph = {
    val descendants: Set[TableName] = model.fileIndex.filter(from).getTableNames.flatMap{getAllDescendants(_)}
    val ascendants: Set[TableName] = model.fileIndex.filter(to).getTableNames.flatMap{getAllAscendants(_)}
    val both: Set[TableName] = descendants.intersect(ascendants)
    val res = this.filter{i => both.exists{_==i}}
    res.graph.findCycle match {
      case Some(cycle) => throw new TableGraphException(s"A cycle was found: $cycle. Using --from and --to that produces a cycle is forbidden.")
      case None => res
    }
    res
  }

  def subGraph(items: Seq[ItemName]): TableGraph = {
    if(items.isEmpty){
      this
    }
    else{
      this.filter{i => items.exists{i.isInOrEqual}}
    }
  }

  def subGraph(itemArgs: ItemArgs): TableGraph = {
    itemArgs match {
      case ItemList(list) => subGraph(list)
      case ItemRange(from,to) => subGraph(from, to)
    }
  }

  def subGraphWithParents(itemArgs: ItemArgs): TableGraph = {
    val tables = this.subGraph(itemArgs).vertices
    val parents: Seq[TableName] = recursivelyAddViewsParents(this.getParents(tables), Set())
    val nonViewParents: Set[String] =
      parents
        .filter{
          tableName => model.getTable(tableName) match {
            case None => false
            case Some(t) => !t.isView
          }
        }
        .map{_.toString}
        .toSet
    val itemFilter = new ItemFilter(tables++parents,acceptIfEmpty=false)
    val newModel =
      model
      .filter{itemFilter}
      .filterFileTypes{
        case (_,FileType.CREATE) | (_,FileType.CREATE_SCHEMA) | (_,FileType.VIEW) => true
        case (itemName, _) =>
          !nonViewParents.contains(itemName)
      }

    new TableGraph(
      newModel,
      graph.filter{graph.having(node={n => itemFilter(n)})},
      Some(baseGraph)
    )
  }

  /**
    * Throws an exception if one of the tables specified in <param>items</param> is missing from the graph
    *
    * @param items
    */
  def checkNoMissingTable(items: Seq[ItemName]): Unit = {
    val tableNames: Seq[TableName] = vertices
    val itemFilter = new ItemFilter(items,acceptIfEmpty=false)
    for (v <- tableNames) {
      if (!itemFilter(v.fullName)) {
        throw new TableGraphException(f"The intermediary table ${v.fullName} should belong to the list of arguments $items")
      }
    }
  }


  /**
    * Warning: This method is not equivalent to ![[isNotView]]
    * @param tableName
    * @return true if the table is known and is a view
    */
  private[graph] def isView(tableName: TableName): Boolean = {
    model.getTable(tableName).exists(_.isView)
  }

  /**
    * Warning: This method is not equivalent to ![[isView]]
    * @param tableName
    * @return true if the table is known and is a not view
    */
  private def isNotView(tableName: TableName): Boolean = {
    model.getTable(tableName).exists(!_.isView)
  }

  /**
    * Recursively find the view's parents and add them to the table list.
    * It is possible to specify table names that should be ignored.
    * If a table is ignored, it will not be included in the result.
    * If a view is ignored, its parents will be ignored too (unless another non-ignored view leads to them).
    * @param tables
    * @param ignoredTables
    * @return
    */
  private def recursivelyAddViewsParents(tables: Seq[TableName], ignoredTables: Set[TableName]): Seq[TableName] = {
    val filteredTables = tables.filter{!ignoredTables.contains(_)}
    val views: Seq[TableName] = filteredTables.filter{isView(_)}
    val parents: Seq[TableName] = getParents(views)
    val recurse: Seq[TableName] =
      if (parents.diff(filteredTables).isEmpty){
        Nil
      }
      else{
        recursivelyAddViewsParents(parents, ignoredTables)
      }
    filteredTables++recurse
  }

  /**
    * Return the set of all non-view parents of these tables, by expanding views.
    * The tables in PopulateInfo.ignoredTables are ignored.
    * If a table is ignored, it will not be included in the result.
    * If a view is ignored, its parents will be ignored too (unless another non-ignored view leads to them).
    * @param populateInfo
    */
  def getParentsThroughViews(populateInfo: PopulateInfo): Seq[TableName] = {
    getParentsThroughViews(populateInfo.tableDependencies.toSeq, populateInfo.ignoredTables)
  }

  /**
    * Return the set of all non-view parents of these tables, by expanding views.
    * It is possible to specify table names that should be ignored.
    * If a table is ignored, it will not be included in the result.
    * If a view is ignored, its parents will be ignored too (unless another non-ignored view leads to them).
    * @param parents the parents that we want to expand
    * @param ignoredTables table names to ignore
    * @return
    */
  private[graph] def getParentsThroughViews(parents: Seq[TableName], ignoredTables: Set[TableName]): Seq[TableName] = {
    recursivelyAddViewsParents(parents, ignoredTables).filter{isNotView(_)}
  }

  /**
    * Creates a new graph where the views have been removed, and their edges where merged.
    *
    * For example, if the original graph is A -> V -> C, and if V is a view,
    * then the new graph will be A -> C
    *
    * @param keepViews if a view matches on of these items, we keep it
    * @return
    */
  def withRemovedViews(keepViews: Seq[ItemName] = Nil): TableGraph = {
    val filter: ItemFilter = new ItemFilter(keepViews, acceptIfEmpty = false)
    var newGraph = graph

    for {
      v: TableName <- vertices
      if isView(v) && !filter(v)
    } {
      val newEdges: Seq[DiEdge[TableName]] =
        for {
          parent <- newGraph.getParents(v)
          child  <- newGraph.getChildren(v)
        } yield {
          parent ~> child
        }
      newGraph -= v
      newGraph ++= newEdges
    }

    new TableGraph(model, newGraph, Some(baseGraph))
  }

  def export: TableGraphExport = {
    new TableGraphExport(this)
  }

  override def toString: String = f"TableGraph(${graph.toString()}})"
}


object TableGraph {

  implicit class TraverserExtension(traverser: Graph[TableName, DiEdge]#InnerNodeTraverser){
    def toNodeSeq: Seq[TableName] = traverser.view.map{_.value}.toSeq
  }

  implicit class GraphExtension(graph: Graph[TableName, DiEdge]) {

    def getNodeSeq: Seq[TableName] = {
      graph.nodes.view.map{_.value}.toSeq
    }

    def getParents(v: TableName): Seq[TableName] = {
      graph.get(v).diPredecessors.toSeq.toNodeSeq
    }

    def getChildren(vertex: TableName): Seq[TableName] = {
      graph.get(vertex).diSuccessors.toSeq.toNodeSeq
    }

  }

  implicit class SetExtension(seq: Seq[Graph[TableName, DiEdge]#NodeT]) {
    def toNodeSeq: Seq[TableName] = seq.map{_.value}
  }

  def apply(model: Model): TableGraph = {
    var graph: Graph[TableName, DiEdge] = Graph[TableName, DiEdge]()

    model.tables.foreach{
      case td: TableInfo =>
        graph += TableName(td.fullName)
        graph ++= td.tableDeps.map{t => TableName(t.fullName) ~> TableName(td.fullName)}
    }
    val set1: Set[TableName] = model.fileIndex.getTableNames
    val set2: Set[TableName] = graph.getNodeSeq.toSet
    assert(set1.forall{set2.contains}, s"Please report a bug: the following tables are in the fileIndex and not in the graph: ${set1.diff(set2)}")

    new TableGraph(model, graph)
  }

  /**
    * Build a TableGraph that contains only the items specified by itemArgs
    * and their required dependencies.
    * Table dependencies are propagated through views. This means that
    * if A -> ViewA -> ViewB -> C, then A will kept.
    *
    * Depending on the context, we might want to check if there are missing tables
    * between the items that were specified, or not.
    *
    * @param context
    * @param items
    * @param checkNoMissingTable
    * @return
    */
  def apply(context: FlamyContext, items: Seq[ItemName], checkNoMissingTable: Boolean): TableGraph = {
    apply(context, ItemList(items), checkNoMissingTable)
  }

  /**
    * Build a TableGraph that contains only the items specified by itemArgs
    * and their required dependencies.
    * Table dependencies are propagated through views. This means that
    * if A -> ViewA -> ViewB -> C, then A will kept.
    *
    * Depending on the context, we might want to check if there are missing tables
    * between the items that were specified, or not.
    *
    * @param context
    * @param itemArgs
    * @param checkNoMissingTable
    * @return
    */
  def apply(context: FlamyContext, itemArgs: ItemArgs, checkNoMissingTable: Boolean): TableGraph = {

    /* Check that all requested items are found */
    context.getFileIndex.strictFilter(itemArgs.allItems).get

    val preModel: IncompleteModel = Model.getIncompleteModel(context, Nil)
    val baseGraph: TableGraph = TableGraph(preModel)

    itemArgs match {
      case ItemList(Nil) => baseGraph
      case ItemList(items) =>
        if(checkNoMissingTable){
          baseGraph.subGraph(items).checkNoMissingTable(items)
        }
        baseGraph.subGraphWithParents(itemArgs)
      case ItemRange(from, to) =>
        baseGraph.subGraphWithParents(itemArgs)
    }
  }

  /**
    * Build a graph with a CompleteModel
    *
    * @param context
    * @return
    */
  def getCompleteModelGraph(context: FlamyContext, items: Iterable[ItemName], checkNoMissingTable: Boolean): TableGraph = {
    getCompleteModelGraph(context, ItemList(items.toSeq), checkNoMissingTable)
  }

  /**
    * Build a graph with a CompleteModel
    *
    * @param context
    * @return
    */
  def getCompleteModelGraph(context: FlamyContext, itemArgs: ItemArgs = ItemList(Nil), checkNoMissingTable: Boolean = false): TableGraph = {
    TableGraph(Model.getCompleteModel(context, itemArgs, checkNoMissingTable))
  }


}