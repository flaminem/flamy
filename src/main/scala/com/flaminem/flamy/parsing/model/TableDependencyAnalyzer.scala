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

package com.flaminem.flamy.parsing.model

import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.collection.immutable.TableCollection
import com.flaminem.flamy.model.columns.ColumnValue
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.utils.logging.Logging

/**
  * Created by fpin on 7/13/16.
  */
class TableDependencyAnalyzer(that: TableDependency) extends Logging {

  import TableDependencyAnalyzer._

  private var tableDepDefinitions: TableCollection = TableCollection()

  @throws(classOf[FlamyException])
  def resolveDependencies(tableDefinitions: TableCollection) {
    resolveDependencies(tableDefinitions, that)
  }

  @throws(classOf[FlamyException])
  private[parsing] def mergeTempTableDeps() {
    for (tt <- getTempTableDeps) {
      that.tableDeps.removeValue(tt)
      tt.mergeTempTableDeps()
      that.colDeps ++= tt.colDeps
      that.tableDeps.addAll(tt.tableDeps)
    }
  }

  /**
    * Resolves all the dependency of the table, and check column names for sub-queries and external tables.
    *
    * @param tableDefinitions
    * @param rootTable table on the top of the dependency. Used for user feedback.
    * @throws FlamyException
    *
    **/
  @throws(classOf[FlamyException])
  private def resolveDependencies(tableDefinitions: TableCollection, rootTable: TableDependency) {
    if(!that.alreadyResolved){
      logger.debug("resolving table : " + that)
      resolveTempTables(tableDefinitions, rootTable)
      loadTableDepDefinitions(tableDefinitions, rootTable)
      resolveViews(tableDefinitions, rootTable)
      that.colDeps = that.colDeps.map{resolveColumnDependency}
      that.columns = that.columns.map{resolveColumnValue(_, rootTable)}
      that.columns = that.columns.flatMap{resolveStarColumn(_, rootTable)}
      cleanTempTableDeps()
      mergeTempTableDeps()
      resolveBothColumnDependencies()
      resolvePostColumnDependencies()
      checkColumnNames(rootTable)
      logger.debug("resolved table : " + that)
      that.alreadyResolved=true
    }
  }

  /**
    * Resolve dependencies on all temporary sub-tables
    *
    * @param tableDefinitions
    * @throws FlamyException
    **/
  @throws(classOf[FlamyException])
  private def resolveTempTables(tableDefinitions: TableCollection, rootTable: TableDependency) {
    for (tt <- getTempTableDeps) {
      tt.resolveDependencies(tableDefinitions, rootTable)
    }
  }

  /**
    * Return a string describing the table definitions
    *
    * @return
    */
  private def getTableDefinitionsDescription: Iterable[String] = {
    tableDepDefinitions.getTables.map {
      case table =>
        val cols: Seq[String] = table.columnAndPartitionNames
        val columns: String = if (cols.nonEmpty) cols.mkString("(", ", ", ")") else ""
        table.fullName + columns
    }
  }

  /**
    * Load the tableDependency definitions, and detect if the table has externalTableDeps
 *
    * @param tableDefinitions
    * @param rootTable
    * @throws com.flaminem.flamy.model.exceptions.FlamyException
    */
  @throws(classOf[FlamyException])
  private def loadTableDepDefinitions(tableDefinitions: TableCollection, rootTable: TableDependency): Unit = {
    that.hasExternalTableDeps = false

    tableDepDefinitions = TableCollection()
    tableDepDefinitions ++=
      that.tableDeps.getAllValues.flatMap{
        td =>
          if (td.isTemp) {
            if (td.hasExternalTableDeps) {
              that.hasExternalTableDeps = true
            }
            td::Nil
          }
          else {
            findTableRef(td, tableDefinitions, rootTable) match {
              case None =>
                that.hasExternalTableDeps = true
                Nil
              case Some(definition) =>
                definition::Nil
            }
          }
      }
  }

  @throws(classOf[FlamyException])
  private def resolveViews(tableDefinitions: TableCollection, rootTable: TableDependency) {
    tableDepDefinitions
      .getTables
      .foreach{
        case td: TableDependency if td.isView => td.resolveDependencies(tableDefinitions, td)
        case _ => ()
      }
  }

  @throws(classOf[FlamyException])
  private[parsing] def cleanTempTableDeps() {
    for (tt <- getTempTableDeps) {
      tt.cleanTempTableDeps()
      removeColDepsFromTempTable(tt)
    }
  }

  @throws(classOf[FlamyException])
  /**
    * Remove dependencies that come from a given temporary table.
    */
  private def removeColDepsFromTempTable(tt: TempTableDependency) {
    that.colDeps = that.colDeps.filter{case cd => !isColumnInTable(cd, tt, strict = false)}
    that.bothColDeps = that.bothColDeps.filter{case cd => !isColumnInTable(cd, tt, strict = false)}
  }

  /**
    * Replace stars with corresponding column names (useful for views)
    *
    * @param col
    * @param rootTable the root table from which this method is called, useful for error handling
    * @return
    */
  private def resolveStarColumn(col: Column, rootTable: TableDependency): Iterable[Column] = {
    if(col.columnName == "*") {
      val defs =
        if(tableDepDefinitions.getTables.exists{_.tableName.startsWith("!.union")}) {
          tableDepDefinitions.getTables.take(1)
        }
        else {
          tableDepDefinitions.getTables
        }
      val deps =
        if(that.tableDeps.exists{_.tableName.startsWith("!.union")}) {
          that.tableDeps.take(1)
        }
        else {
          that.tableDeps
        }
      val newCols =
        for {
          t: Table <- defs
          c: Column <- t.columnsAndPartitions
        } yield {
          if(t.isTable){
            new Column(c.columnName, new ColumnDependency(c.columnName, t.tableName, t.schemaName.orNull))
          }
          else{
            new Column(c.columnName, c.value)
          }
        }
      if (defs.size != deps.size) {
        /* There are some unknown tables */
        Seq(col) ++ newCols
      } else {
        newCols
      }
    }
    else if(col.columnName.endsWith("*")) {
      val colName = col.columnName
      val tableName: String = colName.substring(0, colName.length - 2)
      findTableRef(tableName, tableDepDefinitions, rootTable) match {
        case None => col::Nil
        case Some(t) =>
          t.columnsAndPartitions.map{
            c => new Column(c.columnName, new ColumnDependency(c.columnName, t.tableName, t.schemaName.orNull))
          }
      }
    }
    else{
      col::Nil
    }
  }

  private def resolveColumnValue(column: Column, rootTable: TableDependency): Column = {
    column.value match {
      case cd: ColumnDependency => new Column(column.rawColumnName, resolveColumnValue(cd))
      case _ => column
    }
  }

  /**
    * Check that a post column dependency is valid.
    * For that it must be present in the table columns.
    *
    * @param c
    * @param columnSet
    * @throws com.flaminem.flamy.model.exceptions.FlamyException
    */
  @throws(classOf[FlamyException])
  private def resolvePostColumnDependency(c: ColumnDependency, columnSet: Set[Column]): Unit = {
    if(!columnSet.contains(c)){
      throw new FlamyException(s"Unknown column `${c.getFullName}` in DISTRIBUTE BY or ORDER BY clause.")
    }
  }

  /**
    * Check that all post column dependencies are valid.
    *
    * @throws com.flaminem.flamy.model.exceptions.FlamyException
    */
  @throws(classOf[FlamyException])
  private def resolvePostColumnDependencies() {
    val columnSet = that.columns.toSet
    for (pcd <- that.postColDeps.getAllValues) {
      resolvePostColumnDependency(pcd, columnSet)
    }
  }

  /**
    * Check that a BothColumnDependency is valid.
    * For that it must be present in the table columns.
    *
    * @param c
    * @param columnSet
    * @throws com.flaminem.flamy.model.exceptions.FlamyException
    */
  @throws(classOf[FlamyException])
  private def resolveBothColumnDependency(c: ColumnDependency, columnSet: Set[Column]): ColumnDependency = {
    if(!columnSet.contains(c)){
      resolveColumnDependency(c)
    }
    else {
      c
    }
  }


  @throws(classOf[FlamyException])
  private def resolveColumnDependency(c: ColumnDependency): ColumnDependency = {
    resolveColumnValue(c) match {
      case c: ColumnDependency => c
      case v => new ColumnDependency(c)
    }
  }

  /**
    * Check that all "both" column dependencies are valid.
    *
    * @throws com.flaminem.flamy.model.exceptions.FlamyException
    */
  @throws(classOf[FlamyException])
  private def resolveBothColumnDependencies() {
    val columnSet = that.columns.toSet
    that.bothColDeps = that.bothColDeps.map{resolveBothColumnDependency(_, columnSet)}
  }


  /**
    * Check that a column exists in one of the table dependencies, and
    * throw a FlamyException otherwise.
    * If the table has some dependencies whose definition is missing (i.e. hasExternalTableDeps),
    * we do not raise any error since the column could come from there.
    * This is trickier than it seems as we can have a column named table.column,
    * or a column named structColumn.value
    *
    * @param c
    * @throws FlamyException
    */
  //noinspection ScalaStyle
  @throws(classOf[FlamyException])
  private def resolveColumnValue(c: ColumnDependency): ColumnValue = {
    logger.debug(s"starting resolving column dependency $c")
    logger.debug(s"table definitions: $tableDepDefinitions")
    val foundTables =
      tableDepDefinitions.getTables.filter{
        isColumnInTable(c, _, strict = !that.hasExternalTableDeps)
      }
    logger.debug(s"found tables: $foundTables")
    val res =
      foundTables match {
        case Nil if !that.hasExternalTableDeps =>
          /* If the column is not found in any tableDep and the table has no external table dependency */
          if(that.tableName == "!.subquery_expr" && c.getFullName.contains(".")){
            /* For subquery_exprs (where ... in, where exists), we ignore unerecognized columns when they have a table qualifier */
            c
          }
          else {
            throw new FlamyException(s"No table found for column `${c.getFullName}`.\n  Known tables are $getTableDefinitionsDescription")
          }
        case l
          if l.size > 1
          /* If the column is found in more than one tableDep */
          && c.getFullName!="*"
          /* for, now we disable check on unions */
          && !tableDepDefinitions.forall{_.fullName.startsWith("!.union")} =>
            throw new FlamyException(s"More than one table are matching column `${c.getFullName}`.\n" +
              s"  Matching tables are ${foundTables.map{_.tableName}.toSeq.sorted.mkString("[", ", ", "]")}")
        case Seq(t) if !that.hasExternalTableDeps =>
          /* If the column is found in exactly one table, we add the table name to the column */
          val table: Table = foundTables.head
          if(table.isTable) {
            new ColumnDependency(c.rawColumnName, table.tableName, table.schemaName.orNull)
          }
          else {
            findColumnInTable(c, table).getOrElse(c).value
          }
        case _ => c
      }
    logger.debug(s"finished resolving column dependency $c -> $res")
    res
  }

  /**
    * Returns true if the column is present in the given table
    *
    * @param c
    * @param table
    * @return
    * @throws FlamyException
    */
  @throws(classOf[FlamyException])
  private def findColumnInTable(c: ColumnDependency, table: Table): Option[Column] = {
    val columnName: String = c.columnName
    val fullTableName: Option[String] = c.getFullTableName
    if (fullTableName.isDefined && !fullTableName.get.equalsIgnoreCase(table.fullName)) {
      /* If the table names exists and the table definition is found but do not match */
      return None
    }
    /* The column may be a struct of the form struct.object.name, so we only match the prefix */
    val prefix = columnName.split("[.]")(0)
    table.columnsAndPartitions.find{ c => prefix.equalsIgnoreCase(c.columnName) }
  }

  private def findTableRef(tableRef: Table, tables: TableCollection, rootTable: TableDependency): Option[Table] = {
    assert(!tableRef.isTable)
    findTableRef(tableRef.fullName, tables, rootTable)
  }

  private def findTableRef(fullName: String, tables: TableCollection, rootTable: TableDependency): Option[Table] = {
    val name = fullName.toLowerCase
    val aTable: Option[Table] = tables.get(name)
    if(aTable.isEmpty){
      FlamyOutput.out.warn(s"Table definition not found : $name in table ${rootTable.fullName}")
    }
    aTable
  }

  private def getTempTableDeps: Iterable[TempTableDependency] =
    that.tableDeps.getAllValues.filter{_.isTemp}.map{
      case t : TempTableDependency => t
      case _ => throw new FlamyException("This is not supposed to happen, temp tables should be of type TempTable")
    }

  @throws(classOf[FlamyException])
  private def checkColumnNames(rootTable: TableDependency): Unit = {

    val columnSeeMultipleTimes =
      that.columns
        .groupBy{_.columnName}
        .filter{ case (colName,cols) => cols.size>1}

    columnSeeMultipleTimes match {
      case seq if seq.nonEmpty && that.isView =>
        throw new FlamyException(
          "The following columns names are used multiple times: " +
            seq.map{case (colName,cols) => colName}.mkString(", ")
        )
      case seq if seq.nonEmpty =>
        for((colName,cols) <- seq) {
          FlamyOutput.out.warn(s"Column name used multiple times: $colName in table ${rootTable.fullName}")
        }
      case _ => ()
    }
  }

  override def toString: String = {
    that.toString
  }

}

object TableDependencyAnalyzer {

  /**
    * Returns true if the column is present in the given table
    *
    * @param c
    * @param table
    * @param strict
    * @return
    * @throws FlamyException
    */
  @throws(classOf[FlamyException])
  def isColumnInTable(c: ColumnDependency, table: Table, strict: Boolean): Boolean = {
    val columnName: String = c.columnName
    val fullTableName: Option[String] = c.getFullTableName
    if (fullTableName.isDefined && !fullTableName.get.equalsIgnoreCase(table.fullName)) {
      /* If the table names exists and the table definition is found but do not match */
      return false
    }
    if (columnName == "*") {
      /* If the table column matches everything */
      return true
    }
    /* The column may be a struct of the form struct.object.name, so we only match the prefix */
    val prefix = columnName.split("[.]")(0)
    for (t <- table.columnAndPartitionNames) {
      if (prefix.equalsIgnoreCase(t)) {
        return true
      }
    }
    if (strict && fullTableName.isDefined) {
      throw new FlamyException(s"The column `$columnName` was not found in the definition of table `${table.fullName}`.\n " +
        s"Known columns are : ${table.columnNames}")
    }
    false
  }

}