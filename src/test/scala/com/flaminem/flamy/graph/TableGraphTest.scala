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
import com.flaminem.flamy.model.core.Model
import com.flaminem.flamy.model.files.FileType
import com.flaminem.flamy.model.names.{ItemName, TableName}
import org.scalatest.{FreeSpec, Matchers}

import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._

/**
 * Created by fpin on 1/2/15.
 */
class TableGraphTest extends FreeSpec with Matchers {

    val context = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/test")

  "test graph printing with isolated node with parent view" in {
    val preModel = Model.getIncompleteModel(context, Nil)
    val items = "db_source"::Nil
    val filter = new ItemFilter(items,acceptIfEmpty=true)
    val g = TableGraph(preModel).filterKeepIngoingEdges{filter}
    /* Some weird space seem to come up */
    val dot = g.export.toLightDot.replaceAll(s"[${9.toChar}]"," ")
    val expected =
      """strict digraph {
        | "db_source.source" -> "db_source.source_view" [color = black]
        | subgraph cluster_db_source {
        |  label = db_source
        |  style = dotted
        |  color = black
        |  fontcolor = black
        |  "db_source.source_view"  [shape = plaintext, label = <<TABLE BORDER='1' CELLBORDER='0' CELLSPACING='0'><TR><TD ALIGN='LEFT'>source_view</TD></TR></TABLE>>, color = black, fontcolor = black]
        |  "db_source.source"  [shape = plaintext, label = <<TABLE BORDER='1' CELLBORDER='0' CELLSPACING='0'><TR><TD ALIGN='LEFT'>source</TD></TR></TABLE>>, color = black, fontcolor = black]
        | }
        |}""".stripMargin

    assert(dot === expected)
  }

  "test Graph" in {
    TableGraph.getCompleteModelGraph(context, Nil, checkNoMissingTable = false)
  }


  "test subGraph" in {
    val preModel = Model.getIncompleteModel(context, Nil)
    val g = TableGraph(preModel).subGraph(Seq("db_dest.dest"), Seq("db_dest.dest1"))
    assert(g.vertices.map{_.fullName}.toSet===Set("db_dest.dest","db_dest.dest1"))
  }

  "test subGraph 2" in {
    val preModel = Model.getIncompleteModel(context, Nil)
    val g = TableGraph(preModel).subGraph(from = Seq("db_source.source"), to = Seq("db_source"))
    assert(g.vertices.map{_.fullName}.toSet === Set("db_source.source", "db_source.source_view"))
  }

  "test subGraph 3" in {
    val preModel = Model.getIncompleteModel(context, Nil)
    val g = TableGraph(preModel).subGraph(from = Seq("db_source.source"), to = Seq("db_dest"))
    assert(
      g.vertices.map{_.fullName}.toSet ===
      Set("db_source.source", "db_source.source_view", "db_dest.dest", "db_dest.dest1", "db_dest.dest2", "db_dest.dest12")
    )
  }

  "test subGraph 4" in {
    val preModel = Model.getIncompleteModel(context, Nil)
    val g = TableGraph(preModel).subGraph(from = Seq("db_source.source", "db_dest.dest2"), to = Seq("db_source.source_view", "db_dest.dest12"))
    assert(
      g.vertices.map{_.fullName}.toSet ===
        Set("db_source.source", "db_source.source_view", "db_dest.dest", "db_dest.dest1", "db_dest.dest2", "db_dest.dest12")
    )
  }

  "subGraph with loops" in {
    val context2 = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/Graph")
    val tableGraph = TableGraph(context2, ItemList(Seq("loop")), checkNoMissingTable = false)
    val subG = tableGraph.subGraph(Seq("loop.view1"))
    assert(subG.vertices === Seq(ItemName("loop.view1")))
  }

  "subGraph with from to generating no loop should succeed" in {
    val context2 = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/Graph")
    val tableGraph = TableGraph(context2, ItemList(Seq("loop")), checkNoMissingTable = false)
    val subG = tableGraph.subGraph(Seq("loop.table0"), Seq("loop.table0"))
    assert(subG.vertices === Seq(ItemName("loop.table0")))
  }

  "subGraph with coherent --from --to within a loop should succeed" in {
    val context2 = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/Graph")
    val tableGraph = TableGraph(context2, ItemList(Seq("loop")), checkNoMissingTable = false)
    val subG = tableGraph.subGraph(Seq("loop.view1"), Seq("loop.view2"))
    assert(subG.vertices === Seq(ItemName("loop.view1"), ItemName("loop.view2")))
  }

  "subGraph without loop with from to" in {
    val context2 = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/Graph")
    val tableGraph = TableGraph(context2, ItemList(Seq("views")), checkNoMissingTable = false)
    val subG = tableGraph.subGraph(Seq("views.table0"), Seq("views.table1"))
    assert(subG.vertices.toSet === Set(ItemName("views.table0"), ItemName("views.view1"), ItemName("views.view2"), ItemName("views.table1")))
  }

  "test graph printing" in {
    val args: Seq[ItemName] = Seq("db_dest", "db_source").map{ItemName(_)}
    val model: Model = Model.getCompleteModel(context,args)
    val g = TableGraph(model)
    val expected =
      "TableGraph(Graph(" +
        "db_dest.dest, " +
        "db_dest.dest1, " +
        "db_dest.dest12, " +
        "db_dest.dest2, " +
        "db_dest.meta_table, " +
        "db_out.table_out1, " +
        "db_out.table_out2, " +
        "db_source.source, " +
        "db_source.source_view, " +
        "db_dest.dest~>db_dest.dest1, " +
        "db_dest.dest~>db_dest.dest2, " +
        "db_dest.dest1~>db_dest.dest12, " +
        "db_dest.dest2~>db_dest.dest12, " +
        "db_out.table_out1~>db_dest.meta_table, " +
        "db_out.table_out2~>db_dest.meta_table, " +
        "db_source.source~>db_source.source_view, " +
        "db_source.source_view~>db_dest.dest" +
        ")})"
    assert(g.toString === expected)
  }

  "test ascendants" in {
    val args: Seq[ItemName] = Seq("db_dest", "db_source").map{ItemName(_)}
    val model: Model = Model.getCompleteModel(context, args)
    val g = TableGraph(model)
    val ascendants =
      g.cycleProofTraversal(
        from = Set("db_source.source", "db_source.source_view"),
        to = Set("db_source.source"),
        g.TraversalDirection.Predecessors
      ).toList
    val expected = Set("db_source.source_view", "db_source.source").map{TableName(_)}
    assert(ascendants.toSet === expected)
  }

  "test descendants" in {
    val args: Seq[ItemName] = Seq("db_dest", "db_source").map{ItemName(_)}
    val model: Model = Model.getCompleteModel(context,args)
    val g = TableGraph(model)
    val descendants =
      g.cycleProofTraversal(
        from = Set("db_source.source"),
        to = Set("db_source.source", "db_source.source_view"),
        g.TraversalDirection.Successors
      ).toList
    val expected = Set("db_source.source_view", "db_source.source").map{TableName(_)}
    assert(descendants.toSet === expected)
  }

  "POPULATE script for parent tables should be removed" in {
    val args: ItemArgs = ItemList(Seq("db_dest.dest").map{ItemName(_)})
    val tableGraph = TableGraph(context, args, checkNoMissingTable = false)
    val expected = Set("db_source.source", "db_source.source_view", "db_dest.dest")
    assert(tableGraph.vertices.map{_.toString}.toSet === expected)
    assert(tableGraph.model.fileIndex.getTableFilesOfType("db_source.source", FileType.POPULATE).isEmpty)
  }

  "POPULATE script for other tables should be kept" in {
    val args: ItemArgs = ItemRange(from = Seq("db_source.source").map{ItemName(_)}, to = Seq("db_source").map{ItemName(_)})
    val tableGraph = TableGraph(context, args, checkNoMissingTable = false)
    val expected = Set("db_source.source", "db_source.source_view")
    assert(tableGraph.vertices.map{_.toString}.toSet === expected)
    assert(
      tableGraph.model.fileIndex.getTableFilesOfType("db_source.source", FileType.POPULATE).nonEmpty,
      "the POPULATE script for db_source.source should be kept"
    )
  }

  "getParentsThroughViews should work" in {
    val args: ItemArgs = ItemList(Nil)
    val tableGraph = TableGraph(context, args, checkNoMissingTable = false)
    val parents = tableGraph.getParents("db_dest.dest")
    val expandedParents: Seq[TableName] = tableGraph.getParentsThroughViews(parents, Set())

    assert(expandedParents === Seq[TableName]("db_source.source"))
//    tableGraph.vertices.foreach{println}
//    tableGraph.edges.foreach{println}
//    assert(tableGraph.model.fileIndex.getTableFilesOfType("db_source.source", FileType.POPULATE).isEmpty)
  }

  "getParentsThroughViews with ignoredTables should work" in {
    val args: ItemArgs = ItemList(Nil)
    val tableGraph = TableGraph(context, args, checkNoMissingTable = false)
    val parents = tableGraph.getParents("db_dest.dest")
    val expandedParents: Seq[TableName] = tableGraph.getParentsThroughViews(parents, Set("db_source.source_view"))

    assert(expandedParents.isEmpty)
    //    tableGraph.vertices.foreach{println}
    //    tableGraph.edges.foreach{println}
    //    assert(tableGraph.model.fileIndex.getTableFilesOfType("db_source.source", FileType.POPULATE).isEmpty)
  }

  "getTopologicallySortedVertices should work" in {
      val args: ItemArgs = ItemList(Nil)
      val tableGraph = TableGraph(context, args, checkNoMissingTable = false)

      val t = tableGraph.getTopologicallySortedVertices.toVector

      assert(t.toSet === tableGraph.vertices.toSet)

      for (i <- t.indices) {
        val (left, right) = t.splitAt(i)
        assert(tableGraph.getChildren(t(i)).filter{tableGraph.isView}.forall {right.contains(_)})
        assert(tableGraph.getChildren(t(i)).filter{tableGraph.isView}.forall {!left.contains(_)})
      }
  }

  "withRemovedViews should be correct" in {
    val context2 = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/Graph")
    val tableGraph = TableGraph(context2, ItemList(Seq("views")), checkNoMissingTable = false)
    val g = tableGraph.withRemovedViews()
    val t0 = TableName("views", "table0")
    val t1 = TableName("views", "table1")
    assert(g.graph === Graph[TableName, DiEdge](t0~>t1))
  }

  "withRemovedViews with a keepViews argument should be correct" in {
    val context2 = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/Graph")
    val tableGraph = TableGraph(context2, ItemList(Seq("views")), checkNoMissingTable = false)
    val g = tableGraph.withRemovedViews(Seq("views.view1"))
    val t0 = TableName("views", "table0")
    val t1 = TableName("views", "table1")
    val v1 = TableName("views", "view1")
    assert(g.graph === Graph[TableName, DiEdge](t0~>v1, v1~>t1))
  }

  "getTopologicallySortedVertices" - {

    "should work on a graph with a loop" in {
      val context2 = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/Graph")
      val tableGraph = TableGraph(context2, ItemList(Seq("views")), checkNoMissingTable = false)
      val t = tableGraph.getTopologicallySortedViews.toVector

      assert(t.toSet === tableGraph.vertices.filter{tableGraph.isView}.toSet)

      for (i <- t.indices) {
        val (left, right) = t.splitAt(i)
        assert(tableGraph.getChildren(t(i)).filter{tableGraph.isView}.forall{right.contains(_)})
        assert(tableGraph.getChildren(t(i)).filter{tableGraph.isView}.forall{!left.contains(_)})
      }
    }


    "should work on a graph with an isolated loop" in {
      val context2 = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/Graph")
      val tableGraph = TableGraph(context2, ItemList(Seq("isolated_loop")), checkNoMissingTable = false)
      val t = tableGraph.getTopologicallySortedViews.toVector

      assert(t.toSet === tableGraph.vertices.filter{tableGraph.isView}.toSet)

      for (i <- t.indices) {
        val (left, right) = t.splitAt(i)
        assert(tableGraph.getChildren(t(i)).filter{tableGraph.isView}.forall {right.contains(_)})
        assert(tableGraph.getChildren(t(i)).filter{tableGraph.isView}.forall {!left.contains(_)})
      }
    }

    "should work on a graph with a loop of views" in {
      val context2 = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/Graph")
      val tableGraph = TableGraph(context2, ItemList(Seq("view_loop")), checkNoMissingTable = false)
      intercept[TableGraphException]{
        tableGraph.getTopologicallySortedViews
      }
    }


    "should work on a graph with an isolated loop of views" in {
      val context2 = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/Graph")
      val tableGraph = TableGraph(context2, ItemList(Seq("isolated_view_loop")), checkNoMissingTable = false)
      intercept[TableGraphException]{
        tableGraph.getTopologicallySortedViews
      }
    }

  }

}
