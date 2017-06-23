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

package com.flaminem.flamy.utils.sql

import scala.collection.generic.GenericTraversableTemplate

/**
  * One row of results from an SQL query.
  */
class ResultRow(val metaData: MetaData, data: Seq[String]) extends Seq[String] with GenericTraversableTemplate[String, Seq] {

  private lazy val indexMap: Map[String, Int] = metaData.columnNames.zipWithIndex.toMap

  override def foreach[U](f: (String) => U): Unit = data.foreach{f}
  override def iterator: Iterator[String] = data.iterator
  override def length: Int = data.length

  override def toString: String = data.mkString(", ")

  /**
    * @param columnIndex
    * @return value contained in the cell with specified <code>columnIndex</code>
    */
  override def apply(columnIndex: Int): String = data(columnIndex)

  /**
    * @param columnLabel
    * @return value contained in the specified <code>columnLabel</code>
    */
  def apply(columnLabel: String): String = {
    indexMap.get(columnLabel.toLowerCase) match {
      case Some(idx) => data(idx)
      case None => throw new IllegalArgumentException(f"The following column name was not found: $columnLabel")
    }
  }


}
