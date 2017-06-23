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

package com.flaminem.flamy.graph.export

import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model.TableType
import com.flaminem.flamy.model.names.TableName

import scala.collection.immutable.HashMap
import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.io.dot._

/**
 * Created by fpin on 1/9/15.
 */
abstract class TableGraphExporter(tableGraph: TableGraph) extends TableGraphBaseExporter[TableName](tableGraph) {

  val graph: Graph[TableName, DiEdge] = tableGraph.graph

  var subGraphs = new HashMap[String,DotSubGraph]

  def formatTableVertex(td : TableName): Seq[Seq[Seq[AlignedString]]]

  private def getSubgraph(td: TableName): DotSubGraph = {
    val subGraphName: String = "cluster_" + td.schemaName
    subGraphs.get(subGraphName) match {
      case Some(g) => g
      case None =>
        val g = createSubgraph(subGraphName,td.schemaName)
        subGraphs += (subGraphName -> g)
        g
    }
  }

  private def createSubgraph(subGraphName: String, schemaName: String): DotSubGraph = {
    val isExternal = model.tables.forall{ t => t.schemaName.fullName != schemaName}
    val color = isExternal match {
      case true => "blue"
      case false => "black"
    }
    DotSubGraph(
      root,
      subGraphName,
      DotAttr("label", schemaName)::
        DotAttr("style", "dotted")::
        DotAttr("color", color)::
        DotAttr("fontcolor", color)::
        Nil)
  }

  final override def edgeTransformer(innerEdge: Graph[TableName,DiEdge]#EdgeT):
  Option[(DotGraph,DotEdgeStmt)] = innerEdge.edge match {
    case DiEdge(source, target) =>
      val targetName = target.value.fullName
      val sourceName = source.value.fullName
      val sourceType: Iterable[TableType] =
        for{
          td <- model.tables.get(targetName).view
          sd <- td.tableDeps
          if sd.fullName == sourceName
        } yield {
          sd.getType
        }
      val color =
        sourceType.headOption match {
          case Some(TableType.EXT) => "blue"
          case _ => "black"
        }
      Some((
        root,
        DotEdgeStmt(source.value.fullName, target.value.fullName,
          DotAttr("color", color)::Nil
        )))
  }

  final override def nodeTransformer(innerNode: Graph[TableName,DiEdge]#NodeT): Option[(DotGraph,DotNodeStmt)] = {
    innerNode.value match {
      case td: TableName =>
        val isExternal = !model.tables.contains(td.fullName)
        val color = isExternal match {
          case true => "blue"
          case false => "black"
        }
        Some((
          getSubgraph(td),
          formatNode(td.fullName, color, formatTableVertex(td))
          ))
    }
  }

  private def formatNode(name: String, color: String, cells: Seq[Seq[Seq[AlignedString]]]): DotNodeStmt = {
      DotNodeStmt(
        name,
        DotAttr("shape", "plaintext")::
          DotAttr("label", formatCells(cells))::
          DotAttr("color", color)::
          DotAttr("fontcolor", color)::
          Nil
      )
  }

  private def formatCells(cells: Seq[Seq[Seq[AlignedString]]]): String = {
    def prefix = "<<TABLE BORDER='1' CELLBORDER='0' CELLSPACING='0'>"
    def postfix = "</TABLE>>"
    def separator = "<HR/>"
    cells.map{formatCell}.filter{_.nonEmpty}.mkString(prefix,separator,postfix)
  }

  /**
   * format a cell, which is a set of lines
 *
   * @param cell
   * @return
   */
  private def formatCell(cell: Seq[Seq[AlignedString]]): String = {
    cell.map{formatRow}.mkString
  }

  private def formatRow(row: Seq[AlignedString]): String = {
    "<TR>" + row.map{case AlignedString(string,alignment) => s"<TD ALIGN='$alignment'>$string</TD>"}.mkString + "</TR>"
  }

}
