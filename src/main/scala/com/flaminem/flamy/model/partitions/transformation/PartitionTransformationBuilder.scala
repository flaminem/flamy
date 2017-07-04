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

import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.parsing.hive.FlamyParsingException
import com.flaminem.flamy.utils.macros.SealedValues

/**
  * Created by fpin on 8/17/16.
  */
sealed trait PartitionTransformationBuilder {

  val prefix: String

  def parse(text: String): Annotation

}

object PartitionTransformationBuilder {

  import com.flaminem.flamy.parsing.ParsingUtils._


  /**
    * "IGNORE TIMESTAMP <tableName>" is an annotation
    * that tells the regen to ignore the timestamps of the partitions of the specified source table.
    *
    * This means that the timestamp of the partitions of the source table will always be considered to
    * be older than the timestamp of the partitions of the destination table being regenerated.
    *
    * However, if no partition in the destination matches the lineage of the partitions from that source,
    * new partitions will still be created.
    *
    */
  case object IgnoreTimestamp extends PartitionTransformationBuilder {

    val prefix = "IGNORE TIMESTAMP"

    val tableNameRE = s"(?i)${z}IGNORE${s}TIMESTAMP($fullTableNamePattern)".r

    def getTableName(text: String): TableName = {
      tableNameRE.findFirstMatchIn(text) match {
        case None =>
          throw new FlamyParsingException(
            s"Expected string ($text) to match: $prefix <SCHEMA_NAME>.<TABLE_NAME>"
          )
        case Some(m) => TableName(m.group(1).trim)
      }
    }

    override def parse(text: String): Annotation = {
      new IgnoreTimestamp(
        text,
        getTableName(text)
      )
    }
  }

  case object Ignore extends PartitionTransformationBuilder {
    val tableNameRE = s"(?i)${z}IGNORE($fullTableNamePattern)".r

    val prefix = "IGNORE"

    def getTableName(text: String): TableName = {
      tableNameRE.findFirstMatchIn(text) match {
        case None =>
          throw new FlamyParsingException(
            s"Expected string ($text) to match: IGNORE schema_name.table_name"
          )
        case Some(m) => TableName(m.group(1).trim)
      }
    }

    override def parse(text: String): Annotation = {
      new Ignore(
        text,
        getTableName(text)
      )
    }
  }

  /* This line must stay after the value declaration or it will be empty */
  val values: Seq[PartitionTransformationBuilder] = SealedValues.values[PartitionTransformationBuilder]

}