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

package com.flaminem.flamy.model.partitions.transformation

import com.flaminem.flamy.model.Table
import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.parsing.hive.FlamyParsingException

/**
  * Created by fpin on 11/16/16.
  */
trait Annotation {

  def text: String
  def table: TableName

  def check(parents: Seq[Table]): Unit

  override def toString: String = text

}


object Annotation {

  def apply(text: String): Annotation = {
    val s = text.trim().toLowerCase()
    PartitionTransformationBuilder.values.find{tpe => s.startsWith(tpe.prefix.toLowerCase)} match {
      case None =>
        throw new FlamyParsingException(
          s"Expected string ($text) to start with one of the following prefixes:" +
            PartitionTransformationBuilder.values.map{_.prefix}.mkString(", ")
        )
      case Some(tpe) => tpe.parse(text)
    }
  }

}
