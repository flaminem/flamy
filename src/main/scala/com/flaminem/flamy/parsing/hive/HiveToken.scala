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

import com.flaminem.flamy.parsing.hive.HiveParserUtils.getName
import com.flaminem.flamy.parsing.model.ColumnDependency
import org.apache.hadoop.hive.ql.parse.{ASTNode, HiveParser}

import scala.util.matching.Regex

/**
  * Created by fpin on 1/30/17.
  */
object HiveToken {

  case object EQUAL {

    def unapply(tree: ASTNode): Option[(ASTNode, ASTNode)] = {
      if (tree.getType == HiveParser.EQUAL){
        val left: ASTNode = tree.getChild(0).asInstanceOf[ASTNode]
        val right: ASTNode = tree.getChild(1).asInstanceOf[ASTNode]
        Some(left -> right)
      }
      else {
        None
      }
    }

  }

  case object TOK_PARTVAL {

    def unapplySeq(tree: ASTNode): Option[(ASTNode, Seq[ASTNode])] = {
      if (tree.getType == HiveParser.TOK_PARTVAL){
        if(tree.getChildCount>1){
          val left: ASTNode = tree.getChild(0).asInstanceOf[ASTNode]
          val right: ASTNode = tree.getChild(1).asInstanceOf[ASTNode]
          Some(left, right::Nil)
        }
        else {
          Some(tree.getChild(0).asInstanceOf[ASTNode], Nil)
        }
      }
      else {
        None
      }
    }

  }


  case object DOT {

    def unapply(tree: ASTNode): Option[(ASTNode, ASTNode)] = {
      if (tree.getType == HiveParser.DOT){
        val left: ASTNode = tree.getChild(0).asInstanceOf[ASTNode]
        val right: ASTNode = tree.getChild(1).asInstanceOf[ASTNode]
        Some(left -> right)
      }
      else {
        None
      }
    }

  }


  case object Identifier {

    def unapply(tree: ASTNode): Option[String] = {
      if (tree.getType == HiveParser.Identifier){
        Some(tree.getText)
      }
      else {
        None
      }
    }

  }


  case object TOK_TABLE_OR_COL {

    def unapply(tree: ASTNode): Option[String] = {
      if (tree.getType == HiveParser.TOK_TABLE_OR_COL){
        Some(tree.getChild(0).getText)
      }
      else {
        None
      }
    }

  }


  case object PartitionVar {

    private val partitionVariableRE: Regex = """\A[$][{]partition:(.+)[}]\z""".r

    def unapply(tree: ASTNode): Option[String] = {
      if (tree.getType == HiveParser.StringLiteral) {
        getName(tree) match {
          case partitionVariableRE(name) => Some(name)
          case _ => None
        }
      }
      else {
        None
      }
    }

  }


  case object Column {

    def unapply(tree: ASTNode): Option[ColumnDependency] = {
      tree match {
        case DOT(TOK_TABLE_OR_COL(table), Identifier(column)) =>
          Some(new ColumnDependency(column, table))
        case TOK_TABLE_OR_COL(column) =>
          Some(new ColumnDependency(column))
        case _ => None
      }
    }

  }


}
