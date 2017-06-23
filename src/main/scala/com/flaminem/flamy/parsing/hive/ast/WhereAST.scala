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

package com.flaminem.flamy.parsing.hive.ast

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.conf.hive.ModelHiveContext
import org.apache.hadoop.hive.ql.parse.{ASTNode, HiveParser, ParseDriver}
import com.flaminem.flamy.parsing.hive.HiveParserUtils._
import com.flaminem.flamy.parsing.hive.ast.WhereTransformer.parseColumnDependency
import com.flaminem.flamy.parsing.model.ColumnDependency

/**
  * Created by fpin on 4/28/17.
  */
class WhereAST(val text: String)(implicit context: FlamyContext) {

  val usedColumns: Seq[ColumnDependency] = {
    tree.recParse{
      case pt if pt.getType == HiveParser.TOK_TABLE_OR_COL || pt.getType == HiveParser.DOT =>
        parseColumnDependency(pt)::Nil
    }
  }

  /* Warning, since the tree is mutable and may be transformed multiple times, we need to recreate a fresh one every time we access it */
  def tree: ASTNode = {
    val query = "SELECT 1 WHERE " + text
    new ParseDriver().parse(query, ModelHiveContext.getLightContext(context).hiveContext)
      .getChild(0).asInstanceOf[ASTNode]
      .findNodesWithTypes(Set(HiveParser.TOK_QUERY, HiveParser.TOK_INSERT), Set(HiveParser.TOK_WHERE))
      .head
      .getChild(0)
      .asInstanceOf[ASTNode]
  }

}


object WhereAST {

  def apply(where: String)(implicit context: FlamyContext): WhereAST = {
    new WhereAST(where)
  }

}



