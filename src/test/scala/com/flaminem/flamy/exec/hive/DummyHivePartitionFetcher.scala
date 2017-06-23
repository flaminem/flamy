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

import com.flaminem.flamy.model._
import com.flaminem.flamy.model.core.Model
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.model.metadata.{SchemaWithInfo, TableWithInfo}
import com.flaminem.flamy.model.names.{SchemaName, TableName, TablePartitionName}
import com.flaminem.flamy.model.partitions._
import com.flaminem.flamy.utils.collection.toPairTraversableLikeExtension
import com.flaminem.flamy.utils.time.TimeUtils

import scala.collection.mutable

/**
  * Mock object only used in test
  * @param model
  * @param desc should contain a sequence of (tablePartitionName, partitionLocation, timestamp["yyyy-MM-dd hh:mm:ss"])
  */
class DummyHivePartitionFetcher(model: Model, desc: Traversable[(String,String,String)]) extends HivePartitionFetcher{

  type Loc = String
  type Time = Long

  val info: mutable.Map[TablePartitionName, (Loc, Time)] = {
    val m: Map[TablePartitionName, (String, Time)] =
      desc
        .map {
          case (partition, loc, time) =>
            val tpName = TablePartitionName(partition)
            val timestamp = TimeUtils.universalTimeToTimeStamp(time)
            (tpName,(loc,timestamp))
        }
        .groupByKey
        .map {
          case (name,list) if list.size==1 => (name,list.head)
          case (name,list) => throw new IllegalArgumentException(f"the partition $name was given ${list.size} times")
        }
    mutable.Map[TablePartitionName, (Loc, Time)]() ++ m
  }

  lazy val tableNameSet: Set[TableName] = {
    info.keys.map{ tp=>TableName(tp.tableName.fullName)}.toSet
  }

  def addPartition(partition: Loc, loc: Loc, time: Loc): Option[(Loc, Time)] = {
    info.put(TablePartitionName(partition), (loc, TimeUtils.universalTimeToTimeStamp(time)))
  }


  override def listSchemaNames: Iterable[SchemaName] = {
    info.keys.map{p => SchemaName(p.schemaName)}.toSeq.distinct
  }

  //TODO: implement this
  override def listSchemasWithInfo: Iterable[SchemaWithInfo] = ???

  // TODO: implement this or create HiveTableInfoFetcher trait?
  override def getTableWithInfo(table: TableName): Option[TableWithInfo] = ???

  override def getPartition(tp: TablePartitionName): Option[PartitionWithInfo] = {
    info.get(tp).map{
      case (loc,time) => new DummyPartitionWithInfo(loc, 0L, 0L, time, tp.partColNames.map {_.toPartitionColumn}: _*)
    }
  }

  override def listPartitionNames(table: TableName): Iterable[TablePartitionName] = {
    info.keys
  }


  private def checkPartitionsKeys(tableName: TableName, partitionKeys: Seq[PartitionKey], tablePartitions: Seq[PartitionWithInfo]) = {
    val partitionKeyNames: Set[String] = partitionKeys.map{_.columnName}.toSet
    for {
      tp <- tablePartitions
    } {
      if (partitionKeyNames!= tp.map{_.columnName}.toSet) {
        throw new FlamyException(s"Error found for table $tableName: The partition $tp does not match the partition keys $partitionKeys")
      }
    }

  }

  override def getTablePartitioningInfo(tableName: TableName): TablePartitioningInfo = {
    val partitionKeys: Seq[PartitionKey] =
      model.getTable(tableName).map{_.partitions}.getOrElse(Nil)

    val tablePartitions: Seq[DummyPartitionWithInfo] =
      info
      .filterKeys{_.isInOrEqual(tableName)}
      .map {
        case (tp,(loc,time)) => new DummyPartitionWithInfo(loc,0L,0L,time,tp.partitionColumns:_*)
      }.toSeq

    checkPartitionsKeys(tableName, partitionKeys, tablePartitions)
    new DummyTablePartitioningInfo(tableName, partitionKeys, tablePartitions)
  }

  override def getTable(tableName: TableName): Option[TableInfo] = {
    if(tableNameSet.contains(tableName)) {
      Some(new TableInfo(TableType.EXT, tableName))
    }
    else {
      None
    }
  }

  override def listTablesNamesInSchema(schema: SchemaName): Iterable[TableName] = {
    tableNameSet.filter{_.isInSchema(schema)}
  }

  override def clearCache(): Unit = {
    /* no cache used here */
  }

  override def close(): Unit = ()

}
