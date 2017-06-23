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

import com.flaminem.flamy.parsing.hive.HiveParserUtils._
import org.antlr.runtime.CommonToken
import org.apache.hadoop.hive.ql.parse.{ASTNode, HiveParser}

/**
  * Created by fpin on 11/19/16.
  */
//noinspection ScalaStyle
object NodeFactory {

  def Identifier(text: String): ASTNode = {
    new ASTNode(new CommonToken(HiveParser.Identifier, text))
  }

  def TOK_TABLE_OR_COL(name: String): ASTNode = {
    val node = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL"))
    setChildren(node, Identifier(name))
    node
  }

  def TOK_FROM(child: ASTNode): ASTNode = {
    val tok = new ASTNode(new CommonToken(HiveParser.TOK_FROM, "TOK_FROM"))
    setChildren(tok, child)
  }

  def TOK_TABLE_OR_COL(child: ASTNode): ASTNode = {
    val tok = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL"))
    setChildren(tok, child)
  }

  def DOT(tableName: String, columnName: String): ASTNode = {
    val dot = new ASTNode(new CommonToken(HiveParser.DOT, "."))
    setChildren(dot, TOK_TABLE_OR_COL(tableName), Identifier(columnName))
    dot
  }

  def KW_AND(left: ASTNode, right: ASTNode): ASTNode = {
    val tok = new ASTNode(new CommonToken(HiveParser.KW_AND, "AND"))
    setChildren(tok, left, right)
  }

  def EQUAL(left: ASTNode, right: ASTNode): ASTNode = {
    val tok = new ASTNode(new CommonToken(HiveParser.EQUAL, "EQUAL"))
    setChildren(tok, left, right)
  }

  def TOK_CTE(children: ASTNode*): ASTNode = {
    val tok = new ASTNode(new CommonToken(HiveParser.TOK_CTE, "TOK_CTE"))
    setChildren(tok, children:_*)
  }

  def TOK_FUNCTION(children: ASTNode*): ASTNode = {
    val tok = new ASTNode(new CommonToken(HiveParser.TOK_FUNCTION, "TOK_FUNCTION"))
    setChildren(tok, children:_*)
  }

  def TOK_SELEXPR(children: ASTNode*): ASTNode = {
    val tok = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"))
    setChildren(tok, children:_*)
  }

  def TOK_SELECT(children: ASTNode*): ASTNode = {
    val tok = new ASTNode(new CommonToken(HiveParser.TOK_SELECT, "TOK_SELECT"))
    setChildren(tok, children:_*)
  }

  def TOK_JOIN(children: ASTNode*): ASTNode = {
    val tok = new ASTNode(new CommonToken(HiveParser.TOK_JOIN, "TOK_JOIN"))
    setChildren(tok, children:_*)
  }

  def TOK_LEFTOUTERJOIN(children: ASTNode*): ASTNode = {
    val tok = new ASTNode(new CommonToken(HiveParser.TOK_LEFTOUTERJOIN, "TOK_LEFTOUTERJOIN"))
    setChildren(tok, children:_*)
  }

  def TOK_WHERE(children: ASTNode*): ASTNode = {
    val tok = new ASTNode(new CommonToken(HiveParser.TOK_WHERE, "TOK_WHERE"))
    setChildren(tok, children:_*)
  }

  def TOK_SELEXPR(tableName: String, columnName: String): ASTNode = {
    TOK_SELEXPR(DOT(tableName, columnName))
  }

  def Number(num: String): ASTNode = {
    new ASTNode(new CommonToken(HiveParser.Number, num))
  }

  def StringLiteral(num: String): ASTNode = {
    new ASTNode(new CommonToken(HiveParser.StringLiteral, num))
  }

  def TOK_TMP_FILE: ASTNode = {
    new ASTNode(new CommonToken(HiveParser.TOK_TMP_FILE, "TOK_TMP_FILE"))
  }

  def TOK_DIR: ASTNode = {
    val tok = new ASTNode(new CommonToken(HiveParser.TOK_DIR, "TOK_DIR"))
    setChildren(tok, TOK_TMP_FILE)
    tok
  }

  def TOK_DESTINATION(children: ASTNode*): ASTNode = {
    val tok = new ASTNode(new CommonToken(HiveParser.TOK_DESTINATION, "TOK_DESTINATION"))
    setChildren(tok, children:_*)
    tok
  }

  def TOK_SUBQUERY(query: ASTNode, alias: String): ASTNode = {
    assert(query.getType == HiveParser.TOK_QUERY)
    val tok = new ASTNode(new CommonToken(HiveParser.TOK_SUBQUERY, "TOK_SUBQUERY"))
    setChildren(tok, query, Identifier(alias))
  }

}
