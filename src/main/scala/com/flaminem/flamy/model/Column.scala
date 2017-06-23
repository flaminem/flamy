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

package com.flaminem.flamy.model

import com.flaminem.flamy.model.columns.{ColumnValue, ConstantValue, NoValue}
import com.flaminem.flamy.parsing.model.ColumnDependency
import org.apache.hadoop.hive.metastore.api.FieldSchema

/**
 * Created by fpin on 8/4/15.
 */
class Column (
  val rawColumnName: String,
  rawColumnType: Option[String],
  val comment: Option[String],
  val value: ColumnValue
) {

  val columnName: String = this.rawColumnName.toLowerCase

  val columnType: Option[String] = this.rawColumnType.map{_.toLowerCase}

  def copy(
    rawColumnName: String = this.rawColumnName,
    columnType: Option[String] = this.columnType,
    comment: Option[String] = this.comment,
    value: ColumnValue = this.value
  ): Column = {
    new Column(rawColumnName, columnType, comment, value)
  }

  def this(columnName: String) {
    this(columnName, None, None, NoValue)
  }

  def this(columnName: String, value: ColumnValue) {
    this(columnName, None, None, value)
  }

  def this(columnName: String, columnType: String) {
    this(columnName, Option(columnType), None, NoValue)
  }

  def this(columnName: String, columnType: String, comment: String) {
    this(columnName, Option(columnType), Option(comment), NoValue)
  }

  def this(fieldSchema: FieldSchema) {
    this(fieldSchema.getName, Option(fieldSchema.getType), Option(fieldSchema.getComment), NoValue)
  }

  def getComment: String = comment.orNull

  /**
    * Return the base name of the column
    * @return
    */
  def baseName: String = {
    if(columnName.endsWith("*")){
      "*"
    }
    else {
      columnName
    }
  }

  def hasConstantValue: Boolean = {
    value.isInstanceOf[ConstantValue]
  }


  override def equals(o: Any): Boolean = o match {
    case that: Column =>
      this.columnName==that.columnName && this.columnType==that.columnType && this.comment==that.comment
    case _ => false
  }

  override def hashCode: Int = {
    var result: Int = columnName.hashCode
    result = 31 * result + columnType.hashCode
    result = 31 * result + comment.hashCode
    result
  }

  override def toString: String = {
    if (columnType.isEmpty && comment.isEmpty) {
      /* We only diplay aliases when the column is renamed */
      val valueString: String =
        value match {
          case NoValue => ""
          case cd: ColumnDependency if cd.columnName == columnName => ""
          case v => s"=$value"
        }
      rawColumnName + valueString
    }
    else {
      val name: String = "name=" + this.rawColumnName
      val valueString: String = if(value.isEmpty) "" else s", value=$value"
      val typeString: String = if (columnType.isEmpty) "" else s", type=${columnType.get}"
      val commentString: String = if (comment.isEmpty) "" else s", comment=${comment.get}"
      "Column(" + name + valueString + typeString + commentString + ")"
    }
  }

}
