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

package com.flaminem.flamy.utils.prettyprint

/**
 * Sourced from http://stackoverflow.com/questions/7539831/scala-draw-table-to-console
 */
class Tabulator(leftJustify: Boolean = false) {

  def format(table: Seq[Seq[Any]]): String = table match {
    case Seq() => ""
    case _ =>
      val colSizes = table.map{row => row.map{case null => 0 case cell => cell.toString.length}}.transpose.map{_.max}
      val rows = table.map{row => formatRow(row, colSizes)}
      formatRows(rowSeparator(colSizes), rows)
  }

  private def formatRows(rowSeparator: String, rows: Seq[String]): String = (
    rowSeparator ::
      rows.head ::
      rowSeparator ::
      rows.tail.toList :::
      rowSeparator ::
      List()).mkString("\n")

  private def formatRow(row: Seq[Any], colSizes: Seq[Int]) = {
    val leftJustifyModifier = if(leftJustify) "-" else ""
    row
      .zip(colSizes)
      .map{
      case (_,0) => ""
      case (item,size) => ("%" + leftJustifyModifier + size + "s").format(item)
    }
      .mkString("| ", " | ", " |")
  }

  private def rowSeparator(colSizes: Seq[Int]) = colSizes.map{ "-" * _ }.mkString("+-", "-+-", "-+")
}

object Tabulator {

  def format(table: Seq[Seq[Any]], leftJustify: Boolean = false): String = {
    new Tabulator(leftJustify).format(table)
  }

}