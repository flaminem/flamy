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
import com.flaminem.flamy.parsing.hive.{HiveParserUtils, FlamyParsingException}
import com.flaminem.flamy.parsing.model.ColumnDependency
import org.apache.hadoop.hive.ql.parse.{ASTNode, HiveParser, ParseDriver}

/**
  * Created by fpin on 12/10/16.
  */
object WhereTransformer {
  import HiveParserUtils._

  /**
    * Compute the whereASTs that only concern predictable partitions
    * @param wheres
    * @param predictablePartitions
    * @param context
    * @return
    */
  def getApplicableWhereASTs(wheres: Seq[String], predictablePartitions: Seq[ColumnDependency])(implicit context: FlamyContext): Seq[WhereAST] = {
    if(wheres.isEmpty){
      Nil
    }
    else {
      for {
        where <- wheres
        whereAST: WhereAST = WhereAST(where)
        if whereAST.usedColumns.forall{c => predictablePartitions.exists{c.matches}}
      } yield {
        whereAST
      }
    }
  }

  def parseColumnDependency(pt: ASTNode): ColumnDependency = {
    assert(pt.getType == HiveParser.TOK_TABLE_OR_COL || pt.getType == HiveParser.DOT)
    parseDot(pt) match {
      case Seq(col) => new ColumnDependency(col)
      case Seq(table, col) => new ColumnDependency(col, table)
      case Seq(schema, table, col) => new ColumnDependency(col, table, schema)
      case l => throw new FlamyParsingException(s"Cannot recognize column ${l.mkString(".")}")
    }
  }

  /**
    * If this tree has no child of type TOK_WHERE, insert one.
    * @param tree
    * @return
    */
  private def addTokWhereIfAbsent(tree: ASTNode): ASTNode = {
    assert(tree.getType == HiveParser.TOK_INSERT)
    val children = getChildren(tree)
    if(children.exists{_.getType == HiveParser.TOK_WHERE}) {
      tree
    }
    else{
      val (before: Seq[ASTNode], after: Seq[ASTNode]) =
        children.partition{
          case pt if pt.getType == HiveParser.TOK_DESTINATION => true
          case pt if pt.getType == HiveParser.TOK_SELECT => true
          case _ => false
        }
      val tok_where: ASTNode = NodeFactory.TOK_WHERE()
      setChildren(tree, (before :+ tok_where) ++ after : _ *)
    }
  }

  /**
    * Insert in the where clause the given clauses
    * @param tree
    * @param clauses
    * @return
    */
  def addWhereClauses(tree: ASTNode, clauses: Seq[ASTNode]): ASTNode = {
    assert(tree.getType == HiveParser.TOK_INSERT)
    if(clauses.isEmpty){
      tree
    }
    else {
      val newTree = addTokWhereIfAbsent(tree)
      val tok_where = newTree.getChildrenWithTypes(Set(HiveParser.TOK_WHERE)).head
      Option(tok_where.getChild(0)) match {
        case None =>
          setChildren(tok_where, clauses.reduce{NodeFactory.KW_AND})
        case Some(pt) =>
          setChildren(tok_where, clauses.foldLeft(pt.asInstanceOf[ASTNode]){NodeFactory.KW_AND})
      }
      newTree
    }
  }

}
