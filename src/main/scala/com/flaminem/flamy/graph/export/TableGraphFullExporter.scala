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

package com.flaminem.flamy.graph.export

import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model.Column
import com.flaminem.flamy.model.names.TableName

import scala.util.matching.Regex

/**
 * Created by fpin on 1/9/15.
 */
class TableGraphFullExporter(preTableGraph: TableGraph) extends TableGraphExporter(preTableGraph){

  val STRUCT_RE: Regex = "[^,<>&;]+:[^,<>&;]+".r

  def xmlEncode(s: String): String = s.replace("<","&lt;").replace(">","&gt;")

  def formatColumnType(column: Column): String = {
    val s = xmlEncode(column.columnType.getOrElse("").toUpperCase)
    if (s.contains("STRUCT")) {
      var res = s
      for (couple <- STRUCT_RE.findAllIn(s)) {
        val Array(col,typ) = couple.split(':')
        res = res.replace(couple,s"<BR/>  ${col.toLowerCase}:${typ.toUpperCase}")
      }
      res
    }
    else {
      s
    }
  }

  def formatColumn(column: Column): Seq[AlignedString] = {
    val pk = Option(column.getComment) match {
      case Some(comment) if comment.toUpperCase.startsWith("PK") => "\u26B7   "
      case _ => "     "
    }
    Seq(AlignedString(pk + column.columnName), AlignedString("  " + formatColumnType(column)))
  }

  override def formatTableVertex(td: TableName): Seq[Seq[Seq[AlignedString]]] = {
    val nodeName = td.name
    val header: Seq[Seq[AlignedString]] = Seq(Seq(AlignedString(nodeName, Alignment.CENTER)))
    val columns: Seq[Seq[AlignedString]] =
      model.getTable(td.fullName).toSeq.flatMap{table => table.columns.map{formatColumn}}
    val partitions: Seq[Seq[AlignedString]] =
      model.getTable(td.fullName).toSeq.flatMap{table => table.partitions.map{formatColumn}}

    Seq(header,columns,partitions)
  }
}
