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
import com.flaminem.flamy.graph.export.TableGraphLightExporter
import com.flaminem.flamy.model.ItemFilter
import com.flaminem.flamy.model.core.Model
import org.scalatest.FunSuite

import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.io.dot._

/**
 * Created by fpin on 4/13/15.
 */
class TableGraphExporterTest extends FunSuite{


  test("a TableGraph with a single node should be correctly exported") {
    val context = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/test")
    val preModel: Model = Model.getIncompleteModel(context,Nil)
    val items = "db_source.source"::Nil
    val filter = new ItemFilter(items,true)
    val g = TableGraph(preModel).filterKeepIngoingEdges{filter}

    val ge = new TableGraphLightExporter(g)

    val expected =
      """strict digraph {
        |	subgraph cluster_db_source {
        |		label = db_source
        |		style = dotted
        |		color = black
        |		fontcolor = black
        |		"db_source.source"  [shape = plaintext, label = <<TABLE BORDER='1' CELLBORDER='0' CELLSPACING='0'><TR><TD ALIGN='LEFT'>source</TD></TR></TABLE>>, color = black, fontcolor = black]
        |	}
        |}""".stripMargin
    assert(ge.toDot === expected)

  }

  private lazy val root = new DotRootGraph(
    directed = true,
    id = None,
    strict = true
  )

  private def edgeTransformer(innerEdge: Graph[String,DiEdge]#EdgeT): Option[(DotGraph,DotEdgeStmt)] =
    innerEdge.edge match {
    case DiEdge(source, target) =>
      Some((root, DotEdgeStmt(source.value, target.value, Nil)))
  }

  private def nodeTransformer(innerNode: Graph[String,DiEdge]#NodeT): Option[(DotGraph,DotNodeStmt)] =
    innerNode.value match
    {
      case td: String=> Some((root,DotNodeStmt(td,Nil) ))
    }

  test("a graph with a single node should be correctly exported") {
    var g = Graph[String, DiEdge]()
    g += "A"

    val dot = graph2DotExport(g).toDot(root,edgeTransformer=edgeTransformer,cNodeTransformer=Some(nodeTransformer),iNodeTransformer=Some(nodeTransformer))
    val expected = "strict digraph {\n\tA \n}"
    assert(dot==expected)
  }








}
