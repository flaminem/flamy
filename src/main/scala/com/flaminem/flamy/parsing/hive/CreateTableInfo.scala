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

package com.flaminem.flamy.parsing.hive

import java.io.IOException
import java.util

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.conf.hive.ModelHiveContext
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.exceptions.UnexpectedBehaviorException
import org.apache.hadoop.hive.ql.lib._
import org.apache.hadoop.hive.ql.parse._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class CreateTableInfo(context: FlamyContext) extends NodeProcessor{

  import HiveParserUtils._

  private var table: Table = null
  private val columns = ArrayBuffer[Column]()
  private val partitions = ArrayBuffer[PartitionKey]()

  @throws(classOf[SemanticException])
  def process(pt: Node, stack: util.Stack[Node], procCtx: NodeProcessorCtx, nodeOutputs: AnyRef*): AnyRef = {
    pt.getToken.getType match {
      case HiveParser.TOK_CREATETABLE =>
        table = HiveParserUtils.getTable(TableType.TABLE, pt.getChild(0))
      case HiveParser.TOK_TABCOLLIST =>
        pt.getParent.getType match {
          case HiveParser.TOK_CREATETABLE =>
            columns.addAll(HiveParserUtils.getColumns(pt))
          case HiveParser.TOK_TABLEPARTCOLS =>
            partitions.addAll(HiveParserUtils.getColumns(pt).map{new PartitionKey(_)})
          case _ =>
        }
      case _ =>
    }
    null
  }

  @throws(classOf[ParseException])
  @throws(classOf[SemanticException])
  @throws(classOf[IOException])
  //noinspection ScalaStyle
  def parse(query: String): Table = {
    val pd: ParseDriver = new ParseDriver
    val completeTree = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)
    var tree: ASTNode = completeTree
    while ((tree.getToken == null) && (tree.getChildCount > 0)) {
      tree = tree.getChild(0)
    }
    table = null
    columns.clear()
    partitions.clear()
    val rules: util.Map[Rule, NodeProcessor] = new util.LinkedHashMap[Rule, NodeProcessor]
    val disp: Dispatcher = new DefaultRuleDispatcher(this, rules, null)
    val ogw: GraphWalker = new DefaultGraphWalker(disp)
    val topNodes: util.ArrayList[Node] = new util.ArrayList[Node]
    topNodes.add(tree)
    ogw.startWalking(topNodes, null)
    if(table == null) {
      throw new UnexpectedBehaviorException("Could not parse this AST:" + HiveParserUtils.drawTree(completeTree))
    }
    table.columns = columns
    table.partitions = partitions
    table
  }
}
