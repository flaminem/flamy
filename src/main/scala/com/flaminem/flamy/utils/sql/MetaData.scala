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

import com.flaminem.flamy.model.Column

import scala.collection.generic.GenericTraversableTemplate

/**
  * Created by fpin on 8/7/16.
  */
class MetaData(metaData: Seq[Column]) extends Seq[Column] with GenericTraversableTemplate[Column, Seq]  {
  override def length: Int = metaData.length
  override def apply(idx: Int): Column = metaData(idx)
  override def iterator: Iterator[Column] = metaData.iterator

  def columnNames: Seq[String] = metaData.map{_.columnName}

}
