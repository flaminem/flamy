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

import com.flaminem.flamy.model._
import com.flaminem.flamy.model.columns.{ColumnValue, NoValue}
import com.flaminem.flamy.model.exceptions.UnexpectedBehaviorException
import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.parsing.hive.HiveToken.PartitionVar
import com.flaminem.flamy.parsing.hive.ast.NodeFactory
import com.flaminem.flamy.parsing.model.{ColumnDependency, TableDependency}
import org.antlr.runtime.CommonToken
import org.antlr.runtime.tree.{BaseTree, Tree}
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse.{HiveParser, _}

import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
 * Created by fpin on 8/7/15.
 */
object HiveParserUtils {

  /** Important: we declare an implicit conversion from node to ASTNode */
  implicit def treeToASTNode(node: Tree): ASTNode = {
    node.asInstanceOf[ASTNode]
  }
  implicit def nodeToASTNode(node: Node): ASTNode = {
    node.asInstanceOf[ASTNode]
  }

  /**
    * Get the children of a Node, avoid nulls for leaves.
    * @param pt
    * @return
    */
  def getChildren(pt: ASTNode): Seq[ASTNode] = {
    if(pt.getChildCount > 0){
      pt.getChildren.toSeq.map{_.asInstanceOf[ASTNode]}
    }
    else {
      Nil
    }
  }

  /**
    * Set the children of a Node, and returns it.
    * @param pt
    * @return
    */
  def setChildren(pt: ASTNode, children: Node*): ASTNode = {
    for(i <- (0 until pt.getChildCount).reverse){
      pt.deleteChild(i)
    }
    for(child <- children){
      pt.addChild(child)
    }
    pt
  }

  /**
    * Return a fresh copy of the tree
    */
  def copy(tree: ASTNode): ASTNode = {
    val node = new ASTNode(new CommonToken(tree.getType, tree.toString))
    setChildren(node, getChildren(tree).map{copy}:_*)
    node
  }

  def getLastChild(tree: BaseTree): ASTNode =
    tree match {
      case _ if tree.getChildCount == 0 => throw new IllegalArgumentException("This tree has no child")
      case _ => tree.getChild(tree.getChildren.size - 1).asInstanceOf[ASTNode]
    }

  def getTableDependency(tableType: TableType, pt: ASTNode): TableDependency = {
    new TableDependency(getTable(tableType, pt))
  }

  def getTable(tableType: TableType, pt: ASTNode): Table = {
    assert(pt.getType == HiveParser.TOK_TABNAME, "This should not happen, please report a bug.")
    if (pt.getChildCount == 1) {
      /* SELECT * FROM table */
      val tableName: String = getName(pt.getChild(0))
      new Table(tableType, tableName)
    }
    else {
      /* SELECT * FROM schema.table */
      val schemaName: String = getName(pt.getChild(0))
      val tableName: String = getName(pt.getChild(1))
      new Table(tableType, tableName, schemaName)
    }
  }

  /*
     TOK_TAB
     ├──TOK_TABNAME
     │  └──T1
     └──TOK_PARTSPEC
        └──TOK_PARTVAL
           ├──day
           └──2016-01-01
   */
  def getPartitions(pt: ASTNode): Seq[PartitionColumn] = {
    assert(pt.getType == HiveParser.TOK_TAB)
    val tok_partvals: Seq[ASTNode] =
      pt.findNodesWithTypes(
        allowedTypes = Set(HiveParser.TOK_INSERT, HiveParser.TOK_DESTINATION, HiveParser.TOK_TAB, HiveParser.TOK_PARTSPEC),
        finalTypes = Set(HiveParser.TOK_PARTVAL)
      )
    for(tok_partval <- tok_partvals) yield {
      val name: String = getName(tok_partval.getChild(0))
      val value: ColumnValue =
        if (tok_partval.getChildCount > 1) {
          ColumnValue(getName(tok_partval.getChild(1)))
        }
        else {
          NoValue
        }
      new PartitionColumn(name, value)
    }
  }

  @throws(classOf[SemanticException])
  def getDestColumn(pt: ASTNode, aliases: AliasMap, columnCount: Int): Option[Column] = {
    if (HiveParser.TOK_SELEXPR != pt.getType) {
      None
    }
    else {
      getChildren(pt).toList match {
        case expr::_ if expr.getType == HiveParser.TOK_ALLCOLREF =>
          Some(parseStar(expr, aliases))
        case expr::_ if expr.getType == HiveParser.TOK_TRANSFORM /* REDUCE ... */ =>
          None
        case expr::Nil =>
          val (value, alias) = getColumnValue(expr, aliases, columnCount)
          Some(new Column(alias, None, None, value))
        case expr::alias::Nil =>
          val (value, _) = getColumnValue(expr, aliases, columnCount)
          Some(new Column(getName(alias), None, None, value))
        case _ => None
      }
    }
  }


  /**
    * If the value is a partition variable, we don't want to keep the ColumnDependency,
    * as it might be refering to a column name outside of the current subquery scope.
    * @param s
    * @return
    */
  private def getColumnValueNoPartitionVariable(s: String) = {
    ColumnValue(s) match {
      case cd: ColumnDependency => NoValue
      case v => v
    }
  }

  private def getColumnValue(pt: ASTNode, aliases: AliasMap, columnCount: Int): (ColumnValue, String) = {
    pt.getType match {
      case HiveParser.StringLiteral =>
        (getColumnValueNoPartitionVariable(getName(pt)), "_c" + columnCount)
      case HiveParser.Number =>
        (ColumnValue(getName(pt)), "_c" + columnCount)
      case HiveParser.DOT /* SELECT T.col FROM T */ =>
        /* We need to avoid a nasty corner case for map["key"].sub_value */
        val hasBrackets = pt.findNodesWithTypes(Set(HiveParser.DOT), Set(HiveParser.LSQUARE)).nonEmpty
        if(hasBrackets){
          (NoValue, getName(pt.getChild(1)))
        }
        else{
          val col: ColumnDependency = parseDot(pt, aliases)
          (col, col.columnName.split("[.]").last)
        }
      case HiveParser.TOK_TABLE_OR_COL /* SELECT col FROM T */ =>
        val colName = getName(pt.getChild(0))
        (new ColumnDependency(colName), colName)
      case _ =>
        (NoValue, "_c" + columnCount)
    }
  }

  /*
   TOK_INSERT
   ├──TOK_DESTINATION
   │  └──TOK_DIR
   │     └──TOK_TMP_FILE
   ├──TOK_SELECT | TOK_SELECTDI
   │  ├──TOK_SELEXPR
   │  └──[TOK_SELEXPR]
   ...
  */
  @throws(classOf[SemanticException])
  def getDestColumns(tree: ASTNode): Seq[(String, ASTNode)] = {
    assert(tree.getType == HiveParser.TOK_INSERT)
    for{
      tok_select <- tree.getChildrenWithTypes(Set(HiveParser.TOK_SELECT, HiveParser.TOK_SELECTDI))
      (tok_selexpr, i) <- tok_select.getChildrenWithTypes(Set(HiveParser.TOK_SELEXPR)).zipWithIndex
    } yield {
      val col = Option(tok_selexpr.getChild(1)).map{getName(_)}.getOrElse("_c"+i)
      val tree = tok_selexpr.getChild(0).asInstanceOf[ASTNode]
      col -> tree
    }
  }

  /**
    * Get the list of column names defined in an insert
    * @param tree
    * @return
    */
  def getDestColumnNames(tree: ASTNode): Seq[String] = {
    getDestColumns(tree).map{_._1}
  }

  /**
    * Get the list of columns defined in a CREATE TABLE.
    * @param pt
    * @throws org.apache.hadoop.hive.ql.parse.SemanticException
    * @return
    */
  @throws(classOf[SemanticException])
  def getColumns(pt: ASTNode): Seq[Column] = {
    BaseSemanticAnalyzer.getColumns(pt, true).map{
      case col: FieldSchema => new Column(col)
    }
  }

  def findTable(tableRef: String, aliases: AliasMap): Option[Table] = {
    aliases(tableRef)
  }

  def getName(node: ASTNode): String = {
    BaseSemanticAnalyzer.getUnescapedName(node)
  }

  /**
    * Recursively parse multiple dots.
    * In the special case 't.map["key"].sub_value', we return Seq("t", "map").
    *
    * @param tree
    * @return
    */
  def parseDot(tree: ASTNode): Seq[String] = {
    /* We can't use recParse here, because the map["key"].sub_value is too complex for it */
    def aux(pt: ASTNode, res: List[String]): Seq[String] = {
      pt.getType match {
        case HiveParser.TOK_TABLE_OR_COL => getName(pt.getChild(0)) :: res
        case HiveParser.DOT => aux(pt.getChild(0), getName(pt.getChild(1)) :: res)
        case HiveParser.LSQUARE => aux(pt.getChild(0), Nil)
        case _ => throw new UnexpectedBehaviorException()
      }
    }
    aux(tree, Nil)
  }

  def parseDot(pt: ASTNode, aliases: AliasMap): ColumnDependency = {
    /* Two cases are possible:
     * 1) SELECT T.col FROM T
     * or
     * 2) SELECT col.attr FROM T
     */
    val col: Seq[String] = parseDot(pt)
    val tableOrCol = col.head
    /* we determine which case we're in : */
    findTable(tableOrCol, aliases) match {
      case Some(table) =>
        /* 1) SELECT T.col FROM T */
        new ColumnDependency(col(1), table.tableName, table.getSchemaName)
      case None =>
        /* 2) SELECT col.attr FROM T */
        new ColumnDependency(col.mkString("."))
    }

  }

  @throws(classOf[SemanticException])
  def getColumnDependency(pt: ASTNode, aliases: AliasMap): Option[ColumnDependency] = {
    pt.getType match {
      case HiveParser.TOK_ALLCOLREF if pt.getChildCount == 1 =>
        /* SELECT T.* FROM T */
        val col = "*"
        val tableRef = getName(getLastChild(getLastChild(pt)))
        findTable(tableRef, aliases) match {
          case None => throw new SemanticException(ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg("'" + tableRef + "'"))
          case Some(table) => Some(new ColumnDependency(col, table.tableName, table.getSchemaName))
        }
      case HiveParser.TOK_ALLCOLREF =>
        /* SELECT * FROM T */
        Some(new ColumnDependency("*"))
      case HiveParser.DOT =>
        Some(parseDot(pt, aliases))
      case HiveParser.TOK_TABLE_OR_COL =>
        Some(new ColumnDependency(getName(getLastChild(pt))))
      case _ =>
        None
    }
  }

  /**
    * Return a column with the given name
    * @param pt
    * @return
    */
  def getColumn(pt: ASTNode): Option[Column] = {
    pt.getType match {
      case HiveParser.DOT | HiveParser.TOK_TABLE_OR_COL => Some(new Column(getName(getLastChild(pt))))
      case HiveParser.TOK_SELEXPR if pt.getChildCount > 1 => Some(new Column(getName(getLastChild(pt))))
      case HiveParser.TOK_SELEXPR => getColumn(pt.getChild(0))
      case _ => None
    }
  }

  def parseStar(pt: ASTNode, aliases: AliasMap): Column = {
    if (pt.getChildCount == 1) {
      /* SELECT T.* FROM T */
      val tableRef: String = getName(getLastChild(getLastChild(pt)))
      findTable(tableRef, aliases) match {
        case None => throw new SemanticException(ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg("'" + tableRef + "'"))
        case Some(table) => new Column(table.fullName + ".*")
      }
    }
    else {
      /* SELECT * FROM T */
      new Column("*")
    }
  }

  /**
    * Transforms "c1 AND c2 AND ..." into Seq(c1, c2, ...)
    * @param pt
    * @return
    */
  def andToClauses(pt: ASTNode): Seq[ASTNode] = {
    pt.getType match {
      case HiveParser.KW_AND => getChildren(pt).flatMap{andToClauses}
      case _ => pt::Nil
    }
  }

  /**
    * Transforms Seq(c1, c2, ...) into "c1 AND c2 AND ..."
    * @param clauses
    * @return
    */
  def clausesToAnd(clauses: Seq[ASTNode]): Option[ASTNode] = {
    clauses.reduceOption{NodeFactory.KW_AND}
  }

  /**
    * if multi-insert, we check that they insert into the same tableName and return it.
    */
  private def resolveMultiInsertTableNames(tableNames: Seq[TableName]): TableName = {
    if(tableNames.isEmpty) {
      throw new UnexpectedBehaviorException("No insert found")
    }
    else {
      val ss = tableNames.distinct
      if(ss.size > 1){
        throw new FlamyParsingException("When doing multi-insert, all inserts must go to the same table. " +
          s"Found ${ss.take(2).mkString(" and ")}")
      }
      tableNames.head
    }
  }

  /**
    * Return the name of the table being inserted into.
    * @param tree
    * @return
    */
  def getInsertedTableName(tree: ASTNode): TableName = {
    assert(tree.getType == 0)
    val tok_tabnames: Seq[ASTNode] =
      tree.getChild(0).asInstanceOf[ASTNode]
        .findNodesWithTypes(
          Set(
            HiveParser.TOK_QUERY,
            HiveParser.TOK_INSERT,
            HiveParser.TOK_DESTINATION,
            HiveParser.TOK_INSERT_INTO,
            HiveParser.TOK_TAB
          ),
          Set(HiveParser.TOK_TABNAME)
        )
    val tableNames = tok_tabnames
      .map{getChildren(_)}
      .map{
        case Seq(schema, table) => TableName(getName(schema), getName(table))
        case s => throw new UnexpectedBehaviorException(s"Table names should be fully qualified. Got: ${s.mkString(".")}")
      }
    resolveMultiInsertTableNames(tableNames)
  }

  /**
    * Returns true if at least one of the nodes of this tree corresponds to a partition variable
    */
  def getPartitionVariables(tree: ASTNode): Seq[String] = {
    tree.recParse{
      case PartitionVar(name) =>  name::Nil
    }
  }

  def hasPartitionVariables(tree: ASTNode): Boolean = {
    getPartitionVariables(tree).nonEmpty
  }

  implicit class ASTNodeExtension(that: ASTNode) {

    /**
      * Walks down the tree and find all nodes of type in finalTypes
      * that are reachable by only traversing nodes of type in allowedTypes
      *
      * @param allowedTypes
      * @param finalTypes
      * @return
      */
    def findNodesWithTypes(allowedTypes: Set[Int], finalTypes: Set[Int]): Seq[ASTNode] = {
      recParse{
        case pt if finalTypes.contains(pt.getType) => pt::Nil
        case pt if !allowedTypes.contains(pt.getType) => Nil
      }
    }

    /**
      * @param types
      * @return the list of the children of that node that have the specified type.
      */
    def getChildrenWithTypes(types: Set[Int]): Seq[ASTNode] = {
      getChildren(that).filter{pt => types.contains(pt.getType)}
    }

    /**
      * @param tpe
      * @return the list of the children of that node that have the specified type.
      */
    def getChildrenWithType(tpe: Int): Seq[ASTNode] = {
      getChildren(that).filter{_.getType == tpe}
    }


    /**
      * Replace the child with the specified type with the provided optional replacement.
      * If is node has no child with the specified type, nothing is done.
      * @param tpe
      * @param replacement
      * @return
      */
    def replaceChildWithType(tpe: Int, replacement: Option[ASTNode]): ASTNode = {
      val (before, after) = getChildren(that).span{_.getType != tpe}
      if(after.isEmpty){
        that
      }
      else{
        setChildren(that, before++replacement++after.tail:_*)
      }
    }

    /**
      * Apply a function to the children this nodes, and replace them with the result.
      * @param f
      * @return this tree, after the transformation has been applied to its children.
      */
    def transformChildren(f: Function[ASTNode, Seq[ASTNode]]): ASTNode = {
      setChildren(that, getChildren(that).flatMap{f}:_*)
    }

    /**
      * Apply a function to the children this nodes, and replace them with the result.
      * @param f
      * @return this tree, after the transformation has been applied to its children.
      */
    def filterChildren(f: Function[ASTNode, Boolean]): ASTNode = {
      setChildren(that, getChildren(that).filter{f}:_*)
    }


    /**
      * Recursively apply a partial function to a tree, returning the result.
      * If the partial function doesn't match node, we recursively visit its children
      * @param pf
      * @tparam T
      * @return
      */
    def recParse[T](pf: PartialFunction[(ASTNode), Seq[T]]): Seq[T] = {
      recParse(Set[Int]())(pf)
    }

    /**
      * Recursively apply a partial function to a tree, returning the result.
      * If the partial function doesn't match a node, we recursively visit its children
      * @param allowedTypes The recursion will only explore the children of the nodes whose type is in this set.
      * @param pf
      * @tparam T
      * @return
      */
    def recParse[T](allowedTypes: Set[Int])(pf: PartialFunction[(ASTNode), Seq[T]]): Seq[T] = {
      def defaultRecParse(pt: ASTNode): Seq[T] = {
        if(allowedTypes.isEmpty || allowedTypes.contains(that.getType)) {
          getChildren(pt).flatMap{_.recParse(allowedTypes){pf}}
        }
        else {
          Nil
        }
      }
      pf.applyOrElse(that, defaultRecParse)
    }

    /**
      * Recursively apply a partial transformation function to a tree, returning the transformed tree.
      * If the partial function doesn't match a node, we recursively transform its children
      * @param pf
      * @return
      */
    def recTransform(pf: PartialFunction[(ASTNode), Seq[ASTNode]]): Seq[ASTNode] = {
      recTransform(Set[Int]()){pf}
    }

    /**
      * Recursively apply a partial transformation function to a tree, returning the transformed tree.
      * If the partial function doesn't match a node, we recursively transform its children
      * @param allowedTypes The recursion will only explore the children of the nodes whose type is in this set.
      * @param pf
      * @return
      */
    def recTransform(allowedTypes: Set[Int])(pf: PartialFunction[(ASTNode), Seq[ASTNode]]): Seq[ASTNode] = {
      def defaultRecParse(pt: ASTNode): Seq[ASTNode] = {
        if(allowedTypes.isEmpty || allowedTypes.contains(that.getType)) {
          pt.transformChildren{_.recTransform(allowedTypes){pf}}::Nil
        }
        else {
          Nil
        }
      }
      pf.applyOrElse(that, defaultRecParse)
    }

  }





  @throws(classOf[ParseException])
  private def recDrawTree(prefix: String, tree: ASTNode): String = {
    val sb: StringBuilder = new StringBuilder
    if (prefix.length > 0) {
      if (prefix.endsWith("│  ")) {
        sb.append(prefix.substring(0, prefix.length - 3) + "├──")
      }
      else {
        sb.append(prefix.substring(0, prefix.length - 3) + "└──")
      }
    }
    else {
      sb.append(prefix)
    }
    sb.append(tree.toString + "\n")
    tree.getChildren match {
      //noinspection ScalaStyle
      case null => ()
      case children =>
        children.dropRight(1).foreach {
          case child => sb.append(recDrawTree(prefix + "│  ", child.asInstanceOf[ASTNode]))
        }
        sb.append(recDrawTree(prefix + "   ", children.last.asInstanceOf[ASTNode]))
    }
    sb.toString()
  }

  /**
   * Draw a syntactic tree. Used for debugging.
   * @param tree
   * @return
   */
  def drawTree(tree: ASTNode): String = {
    try {
      recDrawTree("", tree)
    }
    catch {
      case e: ParseException =>
        e.printStackTrace()
        ""
    }
  }



}
