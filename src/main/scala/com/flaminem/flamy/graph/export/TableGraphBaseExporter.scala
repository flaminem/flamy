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

import java.io.{File, PrintWriter}

import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model.core.Model
import com.flaminem.flamy.model.names.ItemName

import scala.io.Source
import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.io.dot.{graph2DotExport, _}

/**
  * Created by fpin on 12/5/16.
  */
abstract class TableGraphBaseExporter[V <: ItemName](tableGraph: TableGraph) {

  val model: Model = tableGraph.model

  val root = new DotRootGraph(
    directed = true,
    id = None,
    strict = true
  )

  val graph: Graph[V, DiEdge]

  protected def edgeTransformer(innerEdge: Graph[V,DiEdge]#EdgeT): Option[(DotGraph,DotEdgeStmt)]
  protected def nodeTransformer(innerNode: Graph[V,DiEdge]#NodeT): Option[(DotGraph,DotNodeStmt)]

  final def toDot: String = {
    graph2DotExport(graph).toDot(
      root,
      edgeTransformer=edgeTransformer,
      cNodeTransformer=Some(nodeTransformer),
      iNodeTransformer=Some(nodeTransformer)
    )
  }

  final def exportToPng(path: String): Unit = {
    val file = new File(path + ".dot")
    val writer = new PrintWriter(file)
    writer.write(this.toDot)
    writer.close()

    scala.sys.process.stringSeqToProcess(Seq("dot", "-T", "png", "-o", path + ".png", path + ".dot")).!
  }

  final def exportToSvg(path: String): Unit = {
    val file = new File(path + ".dot")
    val writer = new PrintWriter(file)
    writer.write(this.toDot)
    writer.close()

    scala.sys.process.stringSeqToProcess(Seq("dot", "-T", "svg", "-o", path + ".svg", path + ".dot")).!

    Source.fromFile(path + ".svg").getLines().dropWhile(!_.startsWith("<svg")).foreach{line => FlamyOutput.out.println(line.toString)}
  }

}
