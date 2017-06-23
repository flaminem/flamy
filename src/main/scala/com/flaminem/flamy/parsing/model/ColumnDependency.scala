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

import com.flaminem.flamy.model.Column
import com.flaminem.flamy.model.columns.{ColumnValue, NoValue}
import com.flaminem.flamy.utils.Named

/**
 * Created by fpin on 8/4/15.
 */
class ColumnDependency private (
  override val rawColumnName: String,
  var tableName: Option[String],
  var schemaName: Option[String]
)
extends Column(rawColumnName, None, None, NoValue) with Comparable[ColumnDependency] with Named with ColumnValue {

  override val value: NoValue.type = NoValue

  def this (col: Column) {
    this(col.columnName, None, None)
  }

  def this (col: Column, tableName: String, schema: String) {
    this(col.rawColumnName, Option(tableName), Option(schema))
  }

  def this (name: String) {
    this(name, None,None)
  }

  def this (name: String, tableName: String) {
    this(name, Option(tableName), None)
  }

  def this (name: String, tableName: String, schemaName: String) {
    this(name, Option(tableName), Option(schemaName))
  }

  def getTableName: String = {
    tableName.orNull
  }

  def setTableName (tableName: String) {
    this.tableName = Option(tableName)
  }

  def getSchema: String = schemaName.orNull


  def setSchemaName (schemaName: Option[String]) {
    this.schemaName = schemaName
  }

  def getFullName: String = {
    if (schemaName.isDefined) {
      schemaName.get + "." + tableName.get + "." + rawColumnName
    }
    else if (tableName.isDefined) {
      tableName.get + "." + rawColumnName
    }
    else {
      rawColumnName
    }
  }

  def getName: String = getFullName

  def getFullTableName: Option[String] = {
    if (schemaName.isDefined) {
      Some(schemaName.get + "." + tableName.get)
    }
    else {
      tableName
    }
  }

  override def toString: String = {
    if (columnType.isEmpty && comment.isEmpty) {
      getFullName
    }
    else {
      val name: String = "name=" + rawColumnName
      val tableNameString: String = if (tableName.isEmpty) "" else ", tableName=" + tableName.get
      val schemaNameString: String = if (schemaName.isEmpty) "" else ", schemaName=" + schemaName.get
      val columnTypeString: String = if (columnType.isEmpty) "" else ", type=" + columnType.get
      val commentString: String = if (comment.isEmpty) "" else ", comment=" + comment.get
      "ColumnDependency [" + name + tableNameString + schemaNameString + columnTypeString + commentString + "]"
    }
  }

  def compareTo (that: ColumnDependency): Int = {
    this.getFullName.compareTo(that.getFullName)
  }

  /**
    * Check if a column dependency is compatible with another, possibly more precise column dependency.
    * For example:
    * col matches col, table.col and db.table.col
    * table.col matches table.col and db.table.col
    * db.table.col matches db.table.col
    * @param that
    * @return true if this column can be matched to that column
    */
  def matches(that: ColumnDependency): Boolean = {
    this.columnName == that.columnName &&
      (this.tableName.isEmpty || this.tableName == that.tableName) &&
      (this.schemaName.isEmpty || this.schemaName == that.schemaName)
  }

  // TODO: this method is temporary, only for backwards compatibility
  override def get: String = ???

}
