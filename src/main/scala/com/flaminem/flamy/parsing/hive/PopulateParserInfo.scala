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

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.conf.hive.ModelHiveContext
import com.flaminem.flamy.model.columns.{ColumnValue, NoValue}
import com.flaminem.flamy.model.{Column, TableType}
import com.flaminem.flamy.parsing.model._
import com.flaminem.flamy.utils.collection.immutable.{NamedCollection, UnicityViolationException, UniqueList}
import com.flaminem.flamy.utils.logging.Logging
import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse._

import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.language.implicitConversions

/**
  * An intermediate object used to manipulate a Hive query AST and construct
  * TableDependencies
  */
private class PopulateParserInfo (
  val tableDeps: TableDependencyCollection = new TableDependencyCollection,

  var tableDeps2: DependencyNode = NoDeps,

  var colDeps: Seq[ColumnDependency] = Vector[ColumnDependency](),

  /* We use a UniqueList to ensure that column names are unique */
  var columns: UniqueList[Column] = UniqueList[Column](),

  var postColDeps: Seq[ColumnDependency] = Seq[ColumnDependency](),
  var bothColDeps: Seq[ColumnDependency] = Seq[ColumnDependency](),
  var outputTables: Seq[TableDependency] = Seq[TableDependency](),

  /**
    * The set of declared aliases (eg from CTEs)
    * in SELECT * FROM T1 as T2, that would be T1
    */
  var knownAliases: AliasMap = AliasMap(),

  /**
    * The set of aliases being declared
    * in SELECT * FROM T1 as T2, that would be T2
    * */
  var newAliases: AliasMap = AliasMap()
) extends Logging {

  import HiveParserUtils._

  private def enrichTableInfo(td: TableDependency) {
    td.columns ++= this.columns
    td.colDeps ++= this.colDeps
    td.bothColDeps ++= this.bothColDeps
    td.postColDeps ++= this.postColDeps
    td.tableDeps ++= this.tableDeps
  }

  private def enrichTableInfo(pi: PopulateParserInfo) {
    pi.columns ++= this.columns
    pi.colDeps ++= this.colDeps
    pi.bothColDeps ++= this.bothColDeps
    pi.postColDeps ++= this.postColDeps
    pi.tableDeps ++= this.tableDeps
  }

  private def merge(that: PopulateParserInfo): PopulateParserInfo = {
    new PopulateParserInfo(
      tableDeps = this.tableDeps ++ that.tableDeps ,
      colDeps = this.colDeps ++ that.colDeps ,
      columns = this.columns ++ that.columns ,
      postColDeps = this.postColDeps ++ that.postColDeps ,
      bothColDeps = this.bothColDeps ++ that.bothColDeps ,
      outputTables = this.outputTables ++ that.outputTables
    )
  }


  /**
    * Look up for a table in the alias tables, detect any anomaly, and return the table if found.
    *
    * @param table
    * @param alias
    * @return
    */
  private def registerTable(table: TableDependency, alias: Option[String] = None): Unit = {
    table.getType match {
      case TableType.TEMP => registerTempTable(table,alias)
      case TableType.REF => registerRefTable(table,alias)
      case _ => ()
    }
  }

  private def registerCTE(table: TableDependency, alias: Option[String] = None): Unit = {
    val tableAlias = alias.getOrElse(table.tableName).toLowerCase()
    logger.debug(s"registering CTE table $table as $tableAlias")
    knownAliases += tableAlias -> table
  }

  private def registerRefTable(table: TableDependency, alias: Option[String] = None): Unit = {
    val tableAlias = alias.getOrElse(table.tableName).toLowerCase()
    val knownTable = knownAliases.getOrElse(table.tableName, table)
    logger.debug(s"registering table reference $table as $tableAlias")
    newAliases += tableAlias -> knownTable
    tableDeps.add(knownTable)
  }

  private def registerTempTable(table: TableDependency, alias: Option[String] = None): Unit = {
    val tableAlias = alias.getOrElse(table.tableName).toLowerCase()
    logger.debug(s"registering subQuery table $table as $tableAlias")
    newAliases += tableAlias -> table
    tableDeps.add(table)
  }

  private def parseSubTree(subTree: Node, keepKnownAliases: Boolean, keepNewAliases: Boolean): PopulateParserInfo = {
    val subPI: PopulateParserInfo = new PopulateParserInfo()
    if(keepKnownAliases) {
      subPI.knownAliases.++=(knownAliases)
    }
    if(keepNewAliases) {
      subPI.newAliases ++= newAliases
    }
    subPI.parse(subTree)
  }


  private def parseSubTree(subTree: Node, knownAliases: AliasMap = AliasMap(), newAliases: AliasMap = AliasMap()): PopulateParserInfo = {
    val subPI: PopulateParserInfo = new PopulateParserInfo()
    subPI.knownAliases ++= knownAliases
    subPI.newAliases ++= newAliases
    subPI.parse(subTree)
  }

  private def parseSubTree(subTree: Node): PopulateParserInfo = {
    parseSubTree(subTree, keepKnownAliases = true, keepNewAliases = false)
  }

  /*
      TOK_QUERY
      ├──TOK_FROM
      │  └──...
      ├──TOK_INSERT
      │  ├──TOK_DESTINATION
      │  ...
      └──[TOK_INSERT]
      └──[TOK_CTE]
      (muti-insert will have multiple TOK_INSERT)
   */
  private def parseQuery(pt: ASTNode, dest: Option[TableDependency]): Seq[TableDependency] = {
    assert(pt.getType == HiveParser.TOK_QUERY, "This should not happen, please report a bug.")
    val opt_tok_from: Option[ASTNode] = pt.getChildrenWithTypes(Set(HiveParser.TOK_FROM)).headOption
    val opt_tok_cte: Option[ASTNode] = pt.getChildrenWithTypes(Set(HiveParser.TOK_CTE)).headOption
    val tok_inserts: Seq[ASTNode] = pt.getChildrenWithTypes(Set(HiveParser.TOK_INSERT))

    opt_tok_cte.foreach{recParseCte}
    val optFromPI = opt_tok_from.map{parseSubTree(_)}
    val aliases = newAliases ++ optFromPI.map{_.newAliases}.getOrElse(AliasMap())
    val insertPIs = tok_inserts.map{parseInsert(_, knownAliases, aliases)}

    val insertTables = insertPIs.flatMap{_.outputTables}
    val outputTables: Seq[TableDependency] =
      if(insertTables.nonEmpty) {
        insertPIs.flatMap{pi => pi.outputTables.foreach{pi.enrichTableInfo} ; pi.outputTables}
      }
      else if(dest.nonEmpty) {
        for {
          d <- dest
          insertPI <- insertPIs
        } {
          d.columns ++= insertPI.columns
          d.colDeps ++= insertPI.colDeps
          d.bothColDeps ++= insertPI.bothColDeps
          d.postColDeps ++= insertPI.postColDeps
        }
        dest.toSeq
      }
      else {
        Nil
      }
    outputTables.foreach{
      t =>
        optFromPI.foreach{_.enrichTableInfo(t)}
    }

    if(outputTables.isEmpty){
      /* When we are in a sub-query, we must propagate the information*/
      insertPIs.foreach{_.enrichTableInfo(this)}
      optFromPI.foreach{_.enrichTableInfo(this)}
    }

    outputTables
  }

  /*
    TOK_INSERT
    ├──TOK_DESTINATION
    │  └──TOK_TAB
    │     └──TOK_TABNAME
    │        ├──db1
    │        └──dest
    ├──TOK_SELECT
    │  └──...
    ├──TOK_WHERE
    │   └──..
    ...
    It will either insert into a known table or into a temporary file
   */
  private def parseInsert(pt: ASTNode, knownAliases: AliasMap, newAliases: AliasMap): PopulateParserInfo = {
    assert(pt.getType == HiveParser.TOK_INSERT | pt.getType == HiveParser.TOK_INSERT_INTO, "This should not happen, please report a bug.")
    pt.getChildren.map {
      case p if p.getType == HiveParser.TOK_WHERE => parseWhere(p, knownAliases, newAliases)
      case p => parseSubTree(p, knownAliases, newAliases)
    }.reduce{_ merge _}
  }

  private def parseWhere(subTree: ASTNode, knownAliases: AliasMap = AliasMap(), newAliases: AliasMap = AliasMap()): PopulateParserInfo = {
    val subPI = parseSubTree(subTree.getChild(0), knownAliases, newAliases)
    new PopulateParserInfo(
      colDeps = subPI.colDeps,
      tableDeps = subPI.tableDeps
    )
  }

  private def parseSubQuery(pt: ASTNode): Unit = {
    val alias: String = BaseSemanticAnalyzer.getUnescapedName(pt.getChild(1))
    val subPI: PopulateParserInfo = parseSubTree(pt.getChild(0))
    val td: TableDependency = new TempTableDependency(alias)
    subPI.enrichTableInfo(td)
    registerTable(td, Some(alias))
  }

  private def parseJoin(pt: ASTNode) {
    val leftPI: PopulateParserInfo = parseSubTree(pt.getChild(0))
    val rightPI: PopulateParserInfo = parseSubTree(pt.getChild(1))

    this.newAliases ++= leftPI.newAliases
    this.newAliases ++= rightPI.newAliases

    val on: Option[PopulateParserInfo] = Option(pt.getChild(2)).map{parseSubTree(_, keepKnownAliases = true, keepNewAliases = true)}

    this.tableDeps ++= leftPI.tableDeps
    this.tableDeps ++= rightPI.tableDeps
    this.colDeps ++= (Seq(leftPI, rightPI) ++ on).flatMap{_.colDeps}

    this.tableDeps2 = new Join(leftPI.tableDeps2, rightPI.tableDeps2)
  }

  private def parseLeftSemiJoin(pt: ASTNode): Unit = {
    val leftPI: PopulateParserInfo = parseSubTree(pt.getChild(0))
    val rightPI: PopulateParserInfo = parseSubTree(pt.getChild(1))
    val clausePI: PopulateParserInfo = new PopulateParserInfo()
    clausePI.newAliases ++= leftPI.newAliases
    clausePI.newAliases ++= rightPI.newAliases
    Option(pt.getChild(2)).foreach{clausePI.parse(_)}

    val td = new TempTableDependency("!.semijoin")
    td.colDeps = NamedCollection(clausePI.colDeps:_*)
    td.postColDeps = NamedCollection(postColDeps:_*)
    td.bothColDeps = NamedCollection(bothColDeps:_*)
    td.tableDeps = new TableDependencyCollection(leftPI.tableDeps.toSeq ++ rightPI.tableDeps.toSeq)

    this.newAliases ++= leftPI.newAliases
    this.tableDeps ++= leftPI.tableDeps
    this.colDeps ++= leftPI.colDeps
    this.tableDeps += td

    this.tableDeps2 = new LeftSemiJoin(leftPI.tableDeps2, rightPI.tableDeps2)
  }

  private def parseSubQueryExpr(pt: ASTNode): Unit = {
    val subPI: PopulateParserInfo = parseSubTree(pt.getChild(1), knownAliases = knownAliases, newAliases = newAliases)
    val td: TableDependency = new TempTableDependency("!.subquery_expr")
    /* The columns from the subquery are not kept */
    subPI.columns = UniqueList()
    subPI.enrichTableInfo(td)
    tableDeps.add(td)
    if(pt.getChildren.size()==3) {
      colDeps ++= HiveParserUtils.getColumnDependency(pt.getChild(2), newAliases)
    }
  }

  private def recParseUnion(pt: ASTNode, res: Vector[PopulateParserInfo] = Vector()): Seq[PopulateParserInfo] = {
    if(pt.getChild(0).getType == HiveParser.TOK_UNIONALL || pt.getChild(0).getType == HiveParser.TOK_UNIONDISTINCT){
      recParseUnion(pt.getChild(0), res :+ parseSubTree(pt.getChild(1)))
    }
    else {
      res ++ pt.getChildren.map{parseSubTree(_)}
    }
  }

  private def parseUnion(pt: ASTNode) {
    val subPIs: Seq[PopulateParserInfo] = recParseUnion(pt)
    val distCols: Seq[List[String]] = subPIs.map{_.columns.map{_.baseName}.toList}.filter{!_.contains("*")}.distinct
    if (distCols.size > 1){
      val Seq(a, b) = distCols.take(2)
      throw new SemanticException(
        f"""Schema of both sides of union should match. ${a.mkString("(",", ",")")} differs from: ${b.mkString("(",", ",")")}
       """.stripMargin)
    }
    tableDeps.addAll(
      subPIs.zipWithIndex.map{
        case (subPI,i) =>
          val td = new TempTableDependency("!.union" + i)
          subPI.enrichTableInfo(td)
          td
      }
    )
    colDeps = subPIs.indices.map{i => new ColumnDependency("*", "union" + i, "!")}
    columns :+= new Column("*")
    this.tableDeps2 = Union(subPIs.map{_.tableDeps2}:_*)
  }


  private def parseHaving(subTree: ASTNode): Unit = {
    this.bothColDeps ++= subTree.getChildren.flatMap{parseSubTree(_, keepKnownAliases = false, keepNewAliases = true).colDeps}
  }

  private def parseExpr(pt: ASTNode): ColumnValue = {
    HiveParserUtils.getDestColumn(pt, newAliases, columns.size) match {
      case Some(col) => col.value
      case None => NoValue
    }
  }

  private def parseOrderBy(subTree: ASTNode): Unit = {
    subTree.getChildren.foreach{
      child =>
        val subPI: PopulateParserInfo = new PopulateParserInfo()
        subPI.newAliases.++=(newAliases)
        subPI.parse(child)

        postColDeps ++= subPI.colDeps
    }
  }

  private def parseLateralView(pt: ASTNode) {
    val subPI: PopulateParserInfo = new PopulateParserInfo()
    subPI.knownAliases.++=(knownAliases)
    subPI.parse(pt.getChild(1))
    subPI.parse(pt.getChild(0))
    /* We keep the new aliases found in the subquery */
    newAliases.++=(subPI.newAliases)

    val selExpr: ASTNode = pt.getChild(0).getChild(0)
    val alias: String = BaseSemanticAnalyzer.getUnescapedName(HiveParserUtils.getLastChild(selExpr).getChild(0))
    val newColumns = selExpr.getChildren.tail.dropRight(1).map{node => new Column(HiveParserUtils.getName(node))}
    subPI.columns = UniqueList(newColumns)
    val td: TableDependency = new TempTableDependency(alias)
    subPI.enrichTableInfo(td)
    tableDeps.addAll(subPI.tableDeps)
    registerTable(td, Some(alias))
  }


  @throws(classOf[ParseException])
  @throws(classOf[SemanticException])
  private[parsing] def parse(tree: ASTNode): PopulateParserInfo = {
    var pt = tree
    while ((pt.getToken == null) && (pt.getChildCount > 0)) {
      pt = pt.getChild(0)
    }
    recParse(pt)
    //    outputTables.foreach{enrichTableInfos}
    this
  }


  private def recParseCte(pt: ASTNode) {
    pt.getChildren.foreach{parseCteSubQuery(_)}
  }

  private def parseCteSubQuery(pt: ASTNode) {
    val alias: String = BaseSemanticAnalyzer.getUnescapedName(pt.getChild(1))
    val subPI: PopulateParserInfo = parseSubTree(pt.getChild(0))
    val td: TableDependency = new TempTableDependency(alias)
    subPI.enrichTableInfo(td)
    registerCTE(td, Some(alias))
  }

  //noinspection ScalaStyle
  @throws(classOf[ParseException])
  @throws(classOf[SemanticException])
  private def recParse(pt: ASTNode) {
    /* When we match these, we want to recursively parse the children */
    pt.getType match {
      case HiveParser.TOK_SELEXPR =>
        columns ++= HiveParserUtils.getDestColumn(pt, newAliases, columns.size)
      case _ => ()
    }
    /* When we match these, we DON'T want to recursively parse the children */
    pt.getType match {
      case HiveParser.TOK_CREATEVIEW =>
        val children = getChildren(pt)
        val view: TableDependency = HiveParserUtils.getTableDependency(TableType.VIEW, children.head)
        outputTables ++= parseQuery(children.last, Some(view))
      case HiveParser.TOK_QUERY =>
        outputTables ++= parseQuery(pt, None)
      case HiveParser.TOK_UNIONALL =>
        parseUnion(pt)
      case HiveParser.TOK_UNIONDISTINCT =>
        parseUnion(pt)
      case HiveParser.TOK_JOIN
           | HiveParser.TOK_LEFTOUTERJOIN
           | HiveParser.TOK_RIGHTOUTERJOIN
           | HiveParser.TOK_FULLOUTERJOIN
           | HiveParser.TOK_CROSSJOIN =>
        parseJoin(pt)
      case HiveParser.TOK_SUBQUERY =>
        /* FROM (subquery) as alias */
        parseSubQuery(pt)
      case HiveParser.TOK_SUBQUERY_EXPR =>
        /* semijoin: WHERE col IN (subquery) */
        parseSubQueryExpr(pt)
      case HiveParser.TOK_HAVING => parseHaving(pt)
      case HiveParser.TOK_ORDERBY | HiveParser.TOK_DISTRIBUTEBY | HiveParser.TOK_SORTBY => parseOrderBy(pt)
      case HiveParser.TOK_WINDOWSPEC =>
      /* OVER (window), currently ignored */
      case HiveParser.TOK_LEFTSEMIJOIN =>
        parseLeftSemiJoin(pt)
      case HiveParser.TOK_LATERAL_VIEW | HiveParser.TOK_LATERAL_VIEW_OUTER =>
        parseLateralView(pt)
      case HiveParser.TOK_TABREF =>
        val table: TableDependency = HiveParserUtils.getTableDependency(TableType.REF, pt.getChild(0))
        val alias =
          if (pt.getChildCount > 1) {
            Some(BaseSemanticAnalyzer.getUnescapedName(pt.getChild(1)))
          }
          else {
            None
          }
        registerTable(table,alias)
      case HiveParser.TOK_ALIASLIST =>
        for (child <- pt.getChildren) {
          columns :+= new Column(HiveParserUtils.getName(child))
        }
      case HiveParser.TOK_TAB =>
        val table: TableDependency = HiveParserUtils.getTableDependency(TableType.REF, pt.getChild(0))
        table.partitions = HiveParserUtils.getPartitions(pt)
        outputTables :+= table
      case _ =>
        HiveParserUtils.getColumnDependency(pt, newAliases) match {
          case Some(col) =>
            colDeps :+= col
          case None =>
            HiveParserUtils.getChildren(pt).foreach{recParse(_)}
        }
    }
  }


  override def toString: String = {
    val tableDepsString = if (this.tableDeps.isEmpty) None else Some(s"tableDeps${tableDeps.mkString("[",", ","]")}")
    val columnsString = if (this.columns.isEmpty) None else Some(s"columns${columns.mkString("[",", ","]")}")
    val colDepsString = if (this.colDeps.isEmpty) None else Some(s"colDeps${colDeps.mkString("[",", ","]")}")
    val postColumnDependenciesString = if (this.postColDeps.isEmpty) None else Some(s"postColumnDependencies${postColDeps.mkString("[",", ","]")}")
    val bothColumnDependenciesString = if (this.bothColDeps.isEmpty) None else Some(s"bothColumnDependencies${bothColDeps.mkString("[",", ","]")}")
    val outputTablesString = if (this.outputTables.isEmpty) None else Some(s"outputTables${outputTables.mkString("[",", ","]")}")
    val knownAliasesString = if (this.knownAliases.isEmpty) None else Some(s"knownAliases${knownAliases.mkString("{",", ","}")}")
    val newAliasesString = if (this.newAliases.isEmpty) None else Some(s"newAliases${newAliases.mkString("{",", ","}")}")
    val fields =
      Seq(
        tableDepsString,
        columnsString,
        colDepsString,
        postColumnDependenciesString,
        bothColumnDependenciesString,
        outputTablesString,
        knownAliasesString,
        newAliasesString
      )
    s"PopulateParserInfo("+fields.flatten.mkString(", ")+")"
  }

}

object PopulateParserInfo extends Logging {

  def parseQuery(query: String)(implicit context: FlamyContext): Seq[TableDependency] = {
    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)
    val ppi = new PopulateParserInfo()

//    println(HiveParserUtils.drawTree(tree))

    try {
      ppi.parse(tree)
    }
    catch {
      case e @ UnicityViolationException(elem: Column) =>
        throw new FlamyParsingException(s"The following column name is used more than once: $elem ", e)
    }
    ppi.outputTables
  }

}
