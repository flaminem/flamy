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
import com.flaminem.flamy.model.names.SchemaName
import com.flaminem.flamy.model.{TableInfo, TableType}

import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._
import scalax.collection.io.dot._

/**
  * Created by fpin on 5/29/16.
  */
class TableGraphSchemaExporter(tableGraph: TableGraph) extends TableGraphBaseExporter[SchemaName](tableGraph){

  val graph: Graph[SchemaName, DiEdge] = {
    var g: Graph[SchemaName, DiEdge] = Graph[SchemaName, DiEdge]()
    tableGraph.vertices.foreach{t => g += t.schemaName}
    g ++=
      tableGraph.edges.collect{
        case (parent, child) if parent.schemaName != child.schemaName => parent.schemaName ~> child.schemaName
      }
    val set1: Set[SchemaName] = model.fileIndex.getSchemaNames
    val set2: Set[SchemaName] = g.nodes.toSeq.map{_.value}.toSet
    assert(set1.forall{set2.contains}, s"Please report a bug: the following tables are in the fileIndex and not in the graph: ${set1.diff(set2)}")

    g
  }

  override def edgeTransformer(innerEdge: Graph[SchemaName,DiEdge]#EdgeT):
  Option[(DotGraph,DotEdgeStmt)] = innerEdge.edge match {
    case DiEdge(source, target) =>
      val targetName = target.value.fullName
      val sourceName = source.value.fullName
      val sourceType: Option[TableType] =
        for {
          td <- model.tables.get(targetName)
          sd <- model.tables.get(sourceName)
        } yield {
          sd.getType
        }
      val color = sourceType match {
        case Some(TableType.EXT) => "blue"
        case _ => "black"
      }
      Some((
        root,
        DotEdgeStmt(source.value.fullName, target.value.fullName,
          DotAttr("color",color)::Nil
        )))
  }

  override def nodeTransformer(innerNode: Graph[SchemaName,DiEdge]#NodeT): Option[(DotGraph,DotNodeStmt)] = {
    innerNode.value match {
      case td: SchemaName =>
        val isExternal = !model.tables.contains(td.fullName)
        val color =
          if (isExternal) {
            "blue"
          }
          else {
            "black"
          }
        Some(( root, formatNode(td.fullName,color,td.fullName) ))
    }
  }

  private def formatNode(name: String, color: String, label: String): DotNodeStmt = {
    DotNodeStmt(
      name,
      DotAttr("shape", "box")::
        DotAttr("label", label)::
        DotAttr("color",color)::
        DotAttr("fontcolor",color)::
        Nil
    )
  }


}
