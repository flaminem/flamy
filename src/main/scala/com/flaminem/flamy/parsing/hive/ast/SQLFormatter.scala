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

import com.flaminem.flamy.parsing.hive.HiveParserUtils
import com.flaminem.flamy.utils.macros.SealedValues
import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse.{ASTNode, HiveParser, ParseException}

/**
  * Created by fpin on 9/4/16.
  */
//noinspection ScalaStyle
object SQLFormatter {

  import HiveParserUtils._

  val indent = "  "

  private class Formatter(splitMultiInsert: Boolean) {

    sealed abstract class TreeFormatter(override val tokenType: Int) extends SingleRule[String, String](tokenType)

    def recurseChildren(pt: ASTNode, prefix: String = ""): Seq[String] = {
      getChildren(pt).map {recTransform(_, prefix)}
    }

    private def formatCaseWhen(children: Seq[Node], prefix: String) = {
      children.zipWithIndex.map {
        case (n, i) if i == children.size - 1 => s"\n$prefix${indent}ELSE " + recTransform(n)
        case (n, i) if i % 2 == 0 => s"\n$prefix${indent}WHEN " + recTransform(n)
        case (n, i) if i % 2 == 1 => " THEN " + recTransform(n)
      }
        .mkString("", "", s"\n${prefix}END")
    }

    /*
        TOK_FUNCTION
        ├──between
        ├──[ KW_FALSE | KW_TRUE ]
        ├──TOK_TABLE_OR_COL
        │  └──b
        ├──TOK_TABLE_OR_COL
        │  └──a
        └──TOK_TABLE_OR_COL
           └──c
        if KW_TRUE, then this means NOT BETWEEN
     */
    private def formatBetween(prefix: String, children: Seq[ASTNode]) = {
      val not =
        if (children(1).getType == HiveParser.KW_TRUE) {
          " NOT"
        }
        else {
          ""
        }
      children.drop(3).map {recTransform(_)}.mkString(recTransform(children(2)) + not + " BETWEEN ", " AND ", "")
    }

    private def formatFunction(prefix: String, distinct: Boolean, pt: ASTNode) = {
      val children: Seq[ASTNode] = getChildren(pt)
      children.head match {
        case p if p.getType == HiveParser.TOK_ISNULL =>
          recTransform(children(1)) + " IS NULL"
        case p if p.getType == HiveParser.TOK_ISNOTNULL =>
          recTransform(children(1)) + " IS NOT NULL"
        case p if p.getType == HiveParser.KW_CASE =>
          "CASE " + recTransform(children(1)) + formatCaseWhen(children.drop(2), prefix)
        case p if p.getType == HiveParser.KW_WHEN =>
          "CASE" + formatCaseWhen(children.tail, prefix)
        case p if p.getType == HiveParser.KW_IN =>
          children.drop(2).map {recTransform(_)}.mkString(recTransform(children(1)) + " IN (", ", ", ")")
        case p if p.getType == HiveParser.Identifier && getName(p).equalsIgnoreCase("between") =>
          formatBetween(prefix, children)
        case p =>
          val distinctString =
            if (distinct) {
              "DISTINCT "
            }
            else {
              ""
            }
          children.tail.map {recTransform(_)}.mkString(s"${children.head}($distinctString", ", ", ")")
      }
    }

    /*
       TOK_LATERAL_VIEW
       ├──TOK_SELECT
       │  └──TOK_SELEXPR
       │     ├──TOK_FUNCTION
       │     │  ├──explode
       │     │  └──TOK_TABLE_OR_COL
       │     │     └──mymap
       │     ├──k1
       │     ├──v1
       │     └──TOK_TABALIAS
       │        └──LV1
       └──TOK_TABREF
          ├──TOK_TABNAME
          │  ├──db2
          │  └──source
          └──T1
     */
    private def formatLateralView(prefix: String, pt: ASTNode, name: String) = {
      val table = pt.getChild(1)
      val children = getChildren(pt.getChild(0).getChild(0))
      recTransform(table) + s"\n$prefix$name " + recTransform(children.head) + " " + recTransform(children.last) +
        children.drop(1).dropRight(1).map {recTransform(_)}.mkString(" as ", ", ", "")
    }

    private def formatJoin(prefix: String, pt: ASTNode, join: String) = {
      val children = getChildren(pt)
      val on =
        if (children.size > 2) {
          s"\n$prefix${indent}ON " + recTransform(children(2), s" ")
        }
        else {
          ""
        }
      children.take(2).map {recTransform(_, prefix)}.mkString("", s"\n$prefix$join ", "") + on
    }

    private def formatUnion(prefix: String, pt: ASTNode, union: String) = {
      val children = getChildren(pt)
      children.take(2).map {recTransform(_, prefix)}.mkString("", s"$prefix$union\n", "")
    }

    //  /**
    //    * Try to write the subquery on one single line
    //    * @param query
    //    */
    //  private def formatCompactQuery(query: ASTNode): String = {
    //    assert(query.getType == HiveParser.TOK_QUERY)
    //    val from = query.getChild(0)
    //    val insert = query.getChild(1)
    //    val select = insert.getChild(1)
    //    val sub: String = recurseChildren(select).mkString(s"SELECT ", s", ", " ") + recTransform(from)
    //    s"(\n${indent}$sub)"
    //  }


    private def formatSubQuery(pt: ASTNode, prefix: String): (String, String) = {
      assert(pt.getType == HiveParser.TOK_SUBQUERY)
      val alias = recTransform(pt.getChild(1))
      ("(\n" + recTransform(pt.getChild(0), prefix + indent) + s"$prefix)", alias)
    }

    object default extends Rule[String, String] {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt, prefix).mkString("")
      }
    }

    object TOK_INSERT_INTO extends TreeFormatter(HiveParser.TOK_INSERT_INTO) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString("INSERT INTO ", " ", "")
      }
    }

    object TOK_DESTINATION extends TreeFormatter(HiveParser.TOK_DESTINATION) {
      def apply(pt: ASTNode, prefix: String): String = {
        if(pt.getChild(0).getType == HiveParser.TOK_DIR) {
          /* this is a subquery*/
          ""
        }
        else {
          recurseChildren(pt).mkString("INSERT OVERWRITE ", " ", "")
        }
      }
    }

    object TOK_QUERY extends TreeFormatter(HiveParser.TOK_QUERY) {
      def apply(pt: ASTNode, prefix: String): String = {
        val tok_from: Option[ASTNode] = pt.getChildrenWithTypes(Set(HiveParser.TOK_FROM)).headOption
        val tok_ctes: Seq[ASTNode] = pt.getChildrenWithTypes(Set(HiveParser.TOK_CTE))
        val tok_inserts: Seq[ASTNode] = pt.getChildrenWithTypes(Set(HiveParser.TOK_INSERT))
        if (tok_inserts.size == 1) {
          val insert = tok_inserts.head
          val dest :: select :: rest = getChildren(insert).toList
          (tok_ctes ++ Seq(dest, select) ++ tok_from.toSeq ++ rest).map {recTransform(_, prefix)}.mkString("")
        }
        else {
          if(splitMultiInsert){
            tok_inserts.map{
              insert =>
                val dest :: select :: rest = getChildren(insert).toList
                (tok_ctes ++ Seq(dest, select) ++ tok_from.toSeq ++ rest).map {recTransform(_, prefix)}.mkString("")
            }.mkString(";\n")
          }
          else {
            val new_toks: Seq[ASTNode] = tok_ctes ++ tok_from ++ tok_inserts.flatMap{getChildren}
            new_toks.map{recTransform(_, prefix)}.mkString("")
          }
          /* multi-insert */
        }
      }
    }

    object TOK_SUBQUERY extends TreeFormatter(HiveParser.TOK_SUBQUERY) {
      def apply(pt: ASTNode, prefix: String): String = {
        val (sub, alias) = formatSubQuery(pt, prefix)
        s"$sub $alias"
      }
    }

    object TOK_SUBQUERY_EXPR extends TreeFormatter(HiveParser.TOK_SUBQUERY_EXPR) {
      def apply(pt: ASTNode, prefix: String): String = {
        val col: String = Option(pt.getChild(2)).map{recTransform(_)+" "}.getOrElse("")
        val op = pt.getChild(0).getChild(0).getText
        val tok_query = pt.getChild(1)
        s"$col$op (\n${recTransform(tok_query, prefix.replace("\n","") + indent)})"
      }
    }

    object TOK_CTE extends TreeFormatter(HiveParser.TOK_CTE) {
      def apply(pt: ASTNode, prefix: String): String = {
        getChildren(pt).map {
          p =>
            val (sub, alias) = formatSubQuery(p, prefix)
            s"$alias AS $sub"
        }.mkString("WITH ", "\n, ", "\n")
      }
    }

    object TOK_FROM extends TreeFormatter(HiveParser.TOK_FROM) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt, prefix).mkString(s"${prefix}FROM ", " ", "\n")
      }
    }

    object TOK_TAB extends TreeFormatter(HiveParser.TOK_TAB) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString("TABLE ", " ", "\n")
      }
    }

    object TOK_TABREF extends TreeFormatter(HiveParser.TOK_TABREF) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(" ")
      }
    }

    object TOK_TABNAME extends TreeFormatter(HiveParser.TOK_TABNAME) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(".")
      }
    }

    object TOK_PARTSPEC extends TreeFormatter(HiveParser.TOK_PARTSPEC) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString("PARTITION(", ", ", ")")
      }
    }

    object TOK_PARTVAL extends TreeFormatter(HiveParser.TOK_PARTVAL) {
      override def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(" = ")
      }
    }

    object TOK_SELECT extends TreeFormatter(HiveParser.TOK_SELECT) {
      def apply(pt: ASTNode, prefix: String): String = {
        val newline = s"\n$prefix$indent"
        recurseChildren(pt, s"$prefix$indent").mkString(s"${prefix}SELECT$newline", s",$newline", "\n")
      }
    }

    object TOK_SELECTDI extends TreeFormatter(HiveParser.TOK_SELECTDI) {
      def apply(pt: ASTNode, prefix: String): String = {
        val newline = s"\n$prefix$indent"
        recurseChildren(pt, s"$prefix$indent").mkString(s"${prefix}SELECT DISTINCT$newline", s",$newline", "\n")
      }
    }

    object TOK_SELEXPR extends TreeFormatter(HiveParser.TOK_SELEXPR) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt, prefix).mkString(" as ")
      }
    }

    object TOK_GROUPBY extends TreeFormatter(HiveParser.TOK_GROUPBY) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(s"${prefix}GROUP BY ", ", ", "\n")
      }
    }

    object TOK_ORDERBY extends TreeFormatter(HiveParser.TOK_ORDERBY) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(s"${prefix}ORDER BY ", ", ", "\n")
      }
    }

    object TOK_DISTRIBUTEBY extends TreeFormatter(HiveParser.TOK_DISTRIBUTEBY) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(s"${prefix}DISTRIBUTE BY ", ", ", "\n")
      }
    }

    object TOK_SORTBY extends TreeFormatter(HiveParser.TOK_SORTBY) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(s"${prefix}SORT BY ", ", ", "\n")
      }
    }

    object TOK_CLUSTERBY extends TreeFormatter(HiveParser.TOK_CLUSTERBY) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(s"${prefix}CLUSTER BY ", ", ", "\n")
      }
    }

    object TOK_TABSORTCOLNAMEDESC extends TreeFormatter(HiveParser.TOK_TABSORTCOLNAMEDESC) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString("", "", " DESC")
      }
    }

    object TOK_WHERE extends TreeFormatter(HiveParser.TOK_WHERE) {
      def apply(pt: ASTNode, prefix: String): String = {
        val newPrefix =
          pt.getChild(0).getType match {
            case HiveParser.KW_OR => s"\n$prefix$indent "
            case HiveParser.KW_AND => s"\n$prefix$indent"
            case _ => prefix
          }
        recurseChildren(pt, newPrefix).mkString(s"${prefix}WHERE ", " ", "\n")
      }
    }

    object TOK_HAVING extends TreeFormatter(HiveParser.TOK_HAVING) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt, s"\n$prefix$indent").mkString(s"${prefix}HAVING ", " ", "\n")
      }
    }

    object KW_AND extends TreeFormatter(HiveParser.KW_AND) {
      def apply(pt: ASTNode, prefix: String): String = {
        val newPrefix =
          if (prefix.contains("\n")) {
            prefix
          }
          else {
            " "
          }
        getChildren(pt).map { p =>
          if (p.getType == HiveParser.KW_OR) {
            "(" + recTransform(p, " ") + ")"
          }
          else {
            recTransform(p, prefix)
          }
        }.mkString(s"${newPrefix}AND ")
      }
    }

    object KW_OR extends TreeFormatter(HiveParser.KW_OR) {
      def apply(pt: ASTNode, prefix: String): String = {
        val newPrefix =
          if (prefix.contains("\n")) {
            prefix
          }
          else {
            " "
          }
        getChildren(pt).map { p =>
          val r: String = recTransform(p, prefix)
          if (p.getType == HiveParser.KW_AND) {
            "(" + recTransform(p, " ") + ")"
          }
          else {
            recTransform(p, prefix)
          }
        }.mkString(s"${newPrefix}OR ")
      }
    }

    object KW_NOT extends TreeFormatter(HiveParser.KW_NOT) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt, prefix).mkString("NOT ", "", "")
      }
    }

    object TOK_JOIN extends TreeFormatter(HiveParser.TOK_JOIN) {
      def apply(pt: ASTNode, prefix: String): String = {
        formatJoin(prefix, pt, "JOIN")
      }
    }

    object TOK_CROSSJOIN extends TreeFormatter(HiveParser.TOK_CROSSJOIN) {
      def apply(pt: ASTNode, prefix: String): String = {
        formatJoin(prefix, pt, "CROSS JOIN")
      }
    }

    object TOK_RIGHTOUTERJOIN extends TreeFormatter(HiveParser.TOK_RIGHTOUTERJOIN) {
      def apply(pt: ASTNode, prefix: String): String = {
        formatJoin(prefix, pt, "RIGHT JOIN")
      }
    }

    object TOK_LEFTOUTERJOIN extends TreeFormatter(HiveParser.TOK_LEFTOUTERJOIN) {
      def apply(pt: ASTNode, prefix: String): String = {
        formatJoin(prefix, pt, "LEFT JOIN")
      }
    }

    object TOK_FULLOUTERJOIN extends TreeFormatter(HiveParser.TOK_FULLOUTERJOIN) {
      def apply(pt: ASTNode, prefix: String): String = {
        formatJoin(prefix, pt, "FULL JOIN")
      }
    }

    object TOK_LEFTSEMIJOIN extends TreeFormatter(HiveParser.TOK_LEFTSEMIJOIN) {
      def apply(pt: ASTNode, prefix: String): String = {
        formatJoin(prefix, pt, "LEFT SEMI JOIN")
      }
    }

    object TOK_UNIONDISTINCT extends TreeFormatter(HiveParser.TOK_UNIONDISTINCT) {
      def apply(pt: ASTNode, prefix: String): String = {
        formatUnion(prefix, pt, "UNION DISTINCT")
      }
    }

    object TOK_UNIONALL extends TreeFormatter(HiveParser.TOK_UNIONALL) {
      def apply(pt: ASTNode, prefix: String): String = {
        formatUnion(prefix, pt, "UNION ALL")
      }
    }

    object TOK_LATERAL_VIEW extends TreeFormatter(HiveParser.TOK_LATERAL_VIEW) {
      def apply(pt: ASTNode, prefix: String): String = {
        formatLateralView(prefix, pt, "LATERAL VIEW")
      }
    }

    object TOK_LATERAL_VIEW_OUTER extends TreeFormatter(HiveParser.TOK_LATERAL_VIEW_OUTER) {
      def apply(pt: ASTNode, prefix: String): String = {
        formatLateralView(prefix, pt, "LATERAL VIEW OUTER")
      }
    }

    object Identifier extends TreeFormatter(HiveParser.Identifier) {
      def apply(pt: ASTNode, prefix: String): String = {
        if (pt.getText.contains('.')) {
          "`" + pt.getText + "`"
        }
        else {
          pt.getText
        }
      }
    }

    object StringLiteral extends TreeFormatter(HiveParser.StringLiteral) {
      def apply(pt: ASTNode, prefix: String): String = {
        pt.getText
      }
    }

    object Number extends TreeFormatter(HiveParser.Number) {
      def apply(pt: ASTNode, prefix: String): String = {
        pt.getText
      }
    }

    object KW_TRUE extends TreeFormatter(HiveParser.KW_TRUE) {
      def apply(pt: ASTNode, prefix: String): String = {
        "true"
      }
    }

    object KW_FALSE extends TreeFormatter(HiveParser.KW_FALSE) {
      def apply(pt: ASTNode, prefix: String): String = {
        "false"
      }
    }

    object TOK_ALLCOLREF extends TreeFormatter(HiveParser.TOK_ALLCOLREF) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).headOption.map {_ + "."}.getOrElse("") + "*"
      }
    }

    object DOT extends TreeFormatter(HiveParser.DOT) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(".")
      }
    }

    object EQUAL extends TreeFormatter(HiveParser.EQUAL) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(" = ")
      }
    }

    object GREATERTHAN extends TreeFormatter(HiveParser.GREATERTHAN) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(" > ")
      }
    }

    object GREATERTHANOREQUALTO extends TreeFormatter(HiveParser.GREATERTHANOREQUALTO) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(" >= ")
      }
    }

    object LESSTHAN extends TreeFormatter(HiveParser.LESSTHAN) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(" < ")
      }
    }

    object LESSTHANOREQUALTO extends TreeFormatter(HiveParser.LESSTHANOREQUALTO) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(" <= ")
      }
    }

    object NOTEQUAL extends TreeFormatter(HiveParser.NOTEQUAL) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(" <> ")
      }
    }

    object PLUS extends TreeFormatter(HiveParser.PLUS) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(" + ")
      }
    }

    object STAR extends TreeFormatter(HiveParser.STAR) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(" * ")
      }
    }

    object DIVIDE extends TreeFormatter(HiveParser.DIVIDE) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(" / ")
      }
    }

    object MINUS extends TreeFormatter(HiveParser.MINUS) {
      def apply(pt: ASTNode, prefix: String): String = {
        if (pt.getChildCount == 1) {
          "- " + recTransform(pt.getChild(0))
        }
        else {
          recurseChildren(pt).mkString(" - ")
        }
      }
    }

    object TOK_FUNCTION extends TreeFormatter(HiveParser.TOK_FUNCTION) {
      def apply(pt: ASTNode, prefix: String): String = {
        formatFunction(prefix, distinct = false, pt)
      }
    }

    object TOK_FUNCTIONDI extends TreeFormatter(HiveParser.TOK_FUNCTIONDI) {
      def apply(pt: ASTNode, prefix: String): String = {
        formatFunction(prefix, distinct = true, pt)
      }
    }

    object TOK_FUNCTIONSTAR extends TreeFormatter(HiveParser.TOK_FUNCTIONSTAR) {
      def apply(pt: ASTNode, prefix: String): String = {
        recTransform(pt.getChild(0)) + "(*)"
      }
    }

    object LSQUARE extends TreeFormatter(HiveParser.LSQUARE) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString("", "[", "]")
      }
    }

    object KW_LIKE extends TreeFormatter(HiveParser.KW_LIKE) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(" LIKE ")
      }
    }

    object KW_RLIKE extends TreeFormatter(HiveParser.KW_RLIKE) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(" RLIKE ")
      }
    }

    object TOK_NULL extends TreeFormatter(HiveParser.TOK_NULL) {
      def apply(pt: ASTNode, prefix: String): String = {
        "NULL"
      }
    }

    object TOK_LIMIT extends TreeFormatter(HiveParser.TOK_LIMIT) {
      def apply(pt: ASTNode, prefix: String): String = {
        recurseChildren(pt).mkString(s"${prefix}LIMIT ", "", "\n")
      }
    }

    /* This line must stay after the value declaration or it will be empty */
    val values: Seq[TreeFormatter] = SealedValues.values[TreeFormatter]

    val recTransform: RuleSet[String, String] = new RuleSet[String, String](default, values.toSeq: _*)

    def recTransform(pt: ASTNode): String = recTransform(pt, "")

  }

  /**
    * Transform a parsed tree back into a SQL Query
    *
    * @param tree
    * @return
    */
  def treeToSQL(tree: ASTNode, splitMultiInsert: Boolean = false): String = {
    try {
      new Formatter(splitMultiInsert).recTransform(tree, "")
    }
    catch {
      case e: ParseException =>
        e.printStackTrace()
        ""
    }
  }


}
