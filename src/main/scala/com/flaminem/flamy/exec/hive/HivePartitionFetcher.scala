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

package com.flaminem.flamy.exec.hive

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.names.{SchemaName, TableName, TablePartitionName}
import com.flaminem.flamy.model.partitions.{PartitionWithInfo, TablePartitioningInfo}
import com.flaminem.flamy.utils.logging.Logging
import org.apache.commons.configuration.ConfigurationException

import scala.util.Try

/**
 * Created by fpin on 12/15/14.
 */
trait HivePartitionFetcher extends HiveTableFetcher {

  /**
    * If the implementation uses a cache, clear it.
    */
  def clearCache(): Unit

  def listPartitionNames(table: TableName): Iterable[TablePartitionName]

  def getTablePartitioningInfo(tableName: TableName): TablePartitioningInfo

  def getPartition(tablePartitionName: TablePartitionName): Option[PartitionWithInfo]

  def printAllPartitionsInSchema(schema: SchemaName): Unit = {
    val sb: StringBuilder = new StringBuilder
    sb ++= schema.fullName ++ "\n"
    listTablesNamesInSchema(schema).foreach {
      table =>
        sb ++= "\t" ++ table.name
        val partitions = listPartitionNames(table)
        if (partitions.size == 1){
          sb ++= " [" ++ partitions.head.partitionName ++ "]"
        }
        if (partitions.size > 1){
          sb ++= " [" ++ partitions.head.partitionName ++ " ... " ++ partitions.last.partitionName ++ "]"
        }
        sb ++= "\n"
    }
    System.out.println(sb.toString())
  }

  def printAllPartitions(): Unit = listSchemaNames.foreach{printAllPartitionsInSchema}

  def getLastPartition(t: TableName): Option[PartitionColumn] = getLastPartition(getTablePartitioningInfo(t))

  def getLastPartition(partitions: TablePartitioningInfo): Option[PartitionColumn] = {
    partitions.timedSlices match {
      case (key, values)::Nil => Some(new PartitionColumn(key, values.toList.sortBy(x=>x).lastOption))
      case _ => None
    }
  }

  def printLastPartitions(): Unit = {
    val sb: StringBuilder = new StringBuilder
    listSchemaNames.foreach{
      db =>
        sb++db.fullName++"\n"
        listTablesNamesInSchema(db).foreach{
          table =>
            getLastPartition(table) match {
              case Some(p) => sb++"\t"++table.name++" ["++p.toString++"]"
              case _ => ()
            }
            sb++"\n"
        }
    }
    System.out.println(sb.toString())
  }

}

object HivePartitionFetcher extends Logging{

  /**
   * Creates a new HiveMetaDataFetcher depending on the context.
   *
   * @param context
   * @return
   */
  def apply(context: FlamyContext): HivePartitionFetcher = {
    val fetcherType = context.HIVE_META_FETCHER_TYPE.getProperty.toLowerCase
    logger.debug(f"Getting new HiveMetaDataFetcher of type $fetcherType")
    fetcherType match {
      case "direct" =>
        new DirectHivePartitionFetcher(context)
      case "client" =>
        new ClientHivePartitionFetcher(context)
      case "default" =>
        Try(new DirectHivePartitionFetcher(context))
        .getOrElse(new ClientHivePartitionFetcher(context))
      case _ => throw new ConfigurationException(f"${context.HIVE_META_FETCHER_TYPE.propertyKey} is not well defined.\n" +
        f"Developers: this error should have been prevented by a Configuration Validator")
    }
  }

}
