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

import java.sql.{DriverManager, Statement}

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.model.metadata.{SchemaWithInfo, TableWithInfo}
import com.flaminem.flamy.model.names.{ItemName, SchemaName, TableName, TablePartitionName}
import com.flaminem.flamy.model.partitions.{PartitionWithEagerInfo, PartitionWithInfo, TablePartitioningInfo}
import com.flaminem.flamy.utils.logging.Logging
import com.flaminem.flamy.utils.sql._

import scala.collection.mutable

/**
 * Created by fpin on 11/3/14.
 */
class DirectHivePartitionFetcher(context: FlamyContext) extends HivePartitionFetcher with Logging {

  private final val SCHEMA_NAME = "SCHEMA_NAME"
  private final val SCHEMA_LOCATION_URI = "SCHEMA_LOCATION_URI"

  private final val TABLE_NAME = "TABLE_NAME"
  private final val PARTITION_NAME = "PARTITION_NAME"

  private final val TABLE_CREATE_TIME = "TABLE_CREATE_TIME"
  private final val TABLE_LOCATION = "TABLE_LOCATION"
  private final val TABLE_TYPE = "TABLE_TYPE"
  private final val TABLE_INPUT_FORMAT = "TABLE_INPUT_FORMAT"
  private final val TABLE_OUTPUT_FORMAT = "TABLE_OUTPUT_FORMAT"
  private final val TABLE_SERDE = "TABLE_SERDE"

  private final val PARTITION_CREATE_TIME = "PARTITION_CREATE_TIME"
  private final val PARTITION_LOCATION = "PARTITION_LOCATION"
  private final val PARTITION_INPUT_FORMAT = "PARTITION_INPUT_FORMAT"
  private final val PARTITION_OUTPUT_FORMAT = "PARTITION_OUTPUT_FORMAT"
  private final val PARTITION_SERDE = "PARTITION_SERDE"

  private final val COLUMN_NAME = "COLUMN_NAME"
  private final val COLUMN_TYPE = "COLUMN_TYPE"
  private final val COLUMN_COMMENT = "COMMENT"
  private final val COLUMN_INDEX = "COLUMN_INDEX"

  private final val PARTCOL_NAME = "PARTCOL_NAME"
  private final val PARTCOL_TYPE = "PARTCOL_TYPE"
  private final val PARTCOL_COMMENT = "PARTCOL_COMMENT"
  private final val PARTCOL_INDEX = "PARTCOL_INDEX"

  private final val TABLE_TYPE_VIEW = "VIRTUAL_VIEW"
  private final val TABLE_TYPE_MANAGED = "MANAGED_TABLE"
  private final val TABLE_TYPE_EXTERNAL = "EXTERNAL_TABLE"

  private final val NUM_FILES = "NUM_FILES"
  private final val NUM_PARTS = "NUM_PARTS"
  private final val NUM_TABLES = "NUM_TABLES"
  private final val TOTAL_SIZE = "TOTAL_SIZE"
  private final val MODIFICATION_TIME = "MODIFICATION_TIME"

  val con: SimpleConnection = {
    new SimpleConnection(
      jdbcUri = context.HIVE_METASTORE_JDBC_URI.getProperty,
      jdbcUser = context.HIVE_METASTORE_JDBC_USER.getProperty,
      jdbcPassword = context.HIVE_METASTORE_JDBC_PASSWORD.getProperty
    )
  }

  private def partitionsParametersSubQuery(where: String) =
    s""" SELECT
       | PP."PART_ID",
       | SUM(COALESCE(PP.numFiles, 0)) as numFiles,
       | SUM(COALESCE(PP.totalSize, 0)) as totalSize,
       | MAX(COALESCE(PP.transient_lastDdlTime, 0)) as transient_lastDdlTime
       | FROM "DBS" D
       | JOIN "TBLS" T
       |   ON T."DB_ID" = D."DB_ID"
       | LEFT JOIN "PARTITIONS" P
       |   ON P."TBL_ID" = T."TBL_ID"
       | LEFT JOIN ($partitionParamsQuery) PP
       |   ON PP."PART_ID" = P."PART_ID"
       | $where
       | GROUP BY PP."PART_ID"
       | """.stripMargin

  private def partitionsQuery(where: String) =
    s""" SELECT
       | D."NAME" as "$SCHEMA_NAME",
       | T."TBL_NAME" as "$TABLE_NAME",
       | P."PART_NAME" as "$PARTITION_NAME",
       | P."CREATE_TIME" as "$PARTITION_CREATE_TIME",
       | SP."LOCATION" as "$PARTITION_LOCATION",
       | SP."INPUT_FORMAT" as "$PARTITION_INPUT_FORMAT",
       | SP."OUTPUT_FORMAT" as "$PARTITION_OUTPUT_FORMAT",
       | SDP."SLIB" as "$PARTITION_SERDE",
       | PP.numFiles as "$NUM_FILES",
       | PP.totalSize as "$TOTAL_SIZE",
       | PP.transient_lastDdlTime as "$MODIFICATION_TIME"
       | FROM "DBS" D
       | JOIN "TBLS" T
       |   ON D."DB_ID" = T."DB_ID"
       | LEFT JOIN "PARTITIONS" P
       |   ON P."TBL_ID" = T."TBL_ID"
       | LEFT JOIN "SDS" SP
       |   ON SP."SD_ID" = P."SD_ID"
       | JOIN "SERDES" SDP
       |   ON SDP."SERDE_ID" = SP."SERDE_ID"
       | LEFT JOIN (${partitionsParametersSubQuery(where)}) PP
       |   ON PP."PART_ID" = P."PART_ID"
       | $where
       | """.stripMargin

  private def getPartitionQuery(tp: TablePartitionName) = {
    partitionsQuery(
      where = s""" WHERE D."NAME"='${tp.schemaName.name}' AND T."TBL_NAME"='${tp.tableName.name}' AND P."PART_NAME"='${tp.partitionName}'"""
    )
  }

  private def tablesQuery(where: String) =
    s""" SELECT
       | D."NAME" as "$SCHEMA_NAME",
       | T."TBL_NAME" as "$TABLE_NAME",
       | T."CREATE_TIME" as "$TABLE_CREATE_TIME",
       | T."TBL_TYPE" as "$TABLE_TYPE",
       | ST."LOCATION" as "$TABLE_LOCATION",
       | ST."INPUT_FORMAT" as "$TABLE_INPUT_FORMAT",
       | ST."OUTPUT_FORMAT" as "$TABLE_OUTPUT_FORMAT",
       | SDT."SLIB" as "$TABLE_SERDE",
       | PK."PKEY_NAME" as "$PARTCOL_NAME",
       | PK."PKEY_TYPE" as "$PARTCOL_TYPE",
       | PK."PKEY_COMMENT" as "$PARTCOL_COMMENT",
       | PK."INTEGER_IDX" as "$PARTCOL_INDEX"
       | FROM "DBS" D
       | JOIN "TBLS" T
       | ON T."DB_ID" = D."DB_ID"
       | JOIN "SDS" ST
       | ON ST."SD_ID" = T."SD_ID"
       | JOIN "SERDES" SDT
       | ON SDT."SERDE_ID" = ST."SERDE_ID"
       | LEFT JOIN "PARTITION_KEYS" PK
       | ON PK."TBL_ID" = T."TBL_ID"
       | $where
       | """.stripMargin

  private val partitionParamsQuery =
    s"""  SELECT
      |    "PART_ID",
      |    CASE WHEN "PARAM_KEY" = 'numFiles' THEN CAST("PARAM_VALUE" as BIGINT) ELSE 0 END as numFiles,
      |    CASE WHEN "PARAM_KEY" = 'totalSize' THEN CAST("PARAM_VALUE" as BIGINT) ELSE 0 END as totalSize,
      |    CASE WHEN "PARAM_KEY" = 'transient_lastDdlTime' THEN CAST("PARAM_VALUE" as BIGINT) ELSE 0 END as transient_lastDdlTime
      |  FROM "PARTITION_PARAMS"
      |""".stripMargin

  private val tableParamsQuery =
    s"""  SELECT
      |    "TBL_ID",
      |    CASE WHEN "PARAM_KEY" = 'numFiles' THEN CAST("PARAM_VALUE" as BIGINT) ELSE 0 END as numFiles,
      |    CASE WHEN "PARAM_KEY" = 'totalSize' THEN CAST("PARAM_VALUE" as BIGINT) ELSE 0 END as totalSize,
      |    CASE WHEN "PARAM_KEY" = 'transient_lastDdlTime' THEN CAST("PARAM_VALUE" as BIGINT) ELSE 0 END as transient_lastDdlTime
      |  FROM "TABLE_PARAMS"
      |""".stripMargin

  private def tableParametersQuery(where: String) =
    s""" SELECT
       | MAX(D."NAME") as "$SCHEMA_NAME",
       | MAX(T."TBL_NAME") as "$TABLE_NAME",
       | COUNT(DISTINCT P."PART_ID") as "$NUM_PARTS",
       | COALESCE(SUM(TP.numFiles), 0) + COALESCE(SUM(PP.numFiles), 0) as "$NUM_FILES",
       | COALESCE(SUM(TP.totalSize), 0) + COALESCE(SUM(PP.totalSize), 0) as "$TOTAL_SIZE",
       | GREATEST(COALESCE(MAX(TP.transient_lastDdlTime), 0), COALESCE(MAX(PP.transient_lastDdlTime), 0)) as "$MODIFICATION_TIME"
       | FROM "DBS" D
       | JOIN "TBLS" T
       |   ON T."DB_ID" = D."DB_ID"
       | LEFT JOIN "PARTITIONS" P
       |   ON P."TBL_ID" = T."TBL_ID"
       | LEFT JOIN ($tableParamsQuery) TP
       |   ON TP."TBL_ID" = T."TBL_ID"
       | LEFT JOIN ($partitionParamsQuery) PP
       |   ON PP."PART_ID" = P."PART_ID"
       | $where
       | """.stripMargin

  private val columnsQuery =
    s""" SELECT
       | D."NAME" as "$SCHEMA_NAME",
       | T."TBL_NAME" as "$TABLE_NAME",
       | C."COLUMN_NAME" as "$COLUMN_NAME",
       | C."TYPE_NAME" as "$COLUMN_TYPE",
       | C."COMMENT" as "$COLUMN_COMMENT",
       | C."INTEGER_IDX" as "$COLUMN_INDEX",
       | T."CREATE_TIME",
       | T."TBL_TYPE"
       | FROM "COLUMNS_V2" C
       | JOIN "SDS" S
       | ON S."CD_ID" = C."CD_ID"
       | JOIN "TBLS" T
       | ON T."SD_ID" = S."SD_ID"
       | JOIN "DBS" D
       | ON D."DB_ID" = T."DB_ID"
       | """.stripMargin

  private def tableNamesQuery(where: String) =
    s""" SELECT
       | D."NAME" as "$SCHEMA_NAME",
       | T."TBL_NAME" as "$TABLE_NAME",
       | T."CREATE_TIME",
       | T."TBL_TYPE"
       | FROM "TBLS" T
       | JOIN "DBS" D
       | ON D."DB_ID" = T."DB_ID"
       | $where
       | """.stripMargin

  private val schemasQuery =
    s""" SELECT
       | D."NAME" as "$SCHEMA_NAME",
       | D."DB_LOCATION_URI" as "$SCHEMA_LOCATION_URI"
       | FROM "DBS" D
       | """.stripMargin

  private def schemaParametersQuery(where: String) =
    s""" SELECT
       | MAX(D."NAME") as "$SCHEMA_NAME",
       | MAX(D."DB_LOCATION_URI") as "$SCHEMA_LOCATION_URI",
       | COUNT(DISTINCT T."TBL_ID") as "$NUM_TABLES",
       | COALESCE(SUM(TP.numFiles), 0) + COALESCE(SUM(PP.numFiles), 0) as "$NUM_FILES",
       | COALESCE(SUM(TP.totalSize), 0) + COALESCE(SUM(PP.totalSize), 0) as "$TOTAL_SIZE",
       | GREATEST(COALESCE(MAX(TP.transient_lastDdlTime), 0), COALESCE(MAX(PP.transient_lastDdlTime), 0)) as "$MODIFICATION_TIME"
       | FROM "DBS" D
       | JOIN "TBLS" T
       |   ON T."DB_ID" = D."DB_ID"
       | LEFT JOIN "PARTITIONS" P
       |   ON P."TBL_ID" = T."TBL_ID"
       | LEFT JOIN ($tableParamsQuery) TP
       |   ON TP."TBL_ID" = T."TBL_ID"
       | LEFT JOIN ($partitionParamsQuery) PP
       |   ON PP."PART_ID" = P."PART_ID"
       | $where
       | GROUP BY D."DB_ID"
       | """.stripMargin


  class LazyCachedIndexedData(filterableQuery: String => String) {

    val data: mutable.Map[ItemName, Seq[ResultRow]] = mutable.Map()

    def clearCache(): Unit = {
      data.clear()
    }

    def apply(tableName: TableName): Iterable[ResultRow] = {
      data.getOrElseUpdate(
        tableName,
        {
          val schema = tableName.schemaName.name
          val table = tableName.name
          val filteredQuery = filterableQuery(s""" WHERE D."NAME" = '$schema' AND T."TBL_NAME" = '$table' """)
          logger.debug(s"Executing query:\n $filteredQuery")
          con.executeQuery(filteredQuery)
        }
      )
    }

    def apply(schemaName: SchemaName): Iterable[ResultRow] = {
      data.getOrElseUpdate(
        schemaName,
        {
          val schema = schemaName.name
          val filteredQuery = filterableQuery(s""" WHERE D."NAME" = '$schemaName' """)
          logger.debug(s"Executing query:\n $filteredQuery")
          con.executeQuery(filteredQuery)
        }
      )
    }

    /** Get all rows associated to this TablePartitionName */
    def apply(tp: TablePartitionName): Iterable[ResultRow] = {
      val partSet: Set[String] = tp.partColNames.map{_.toString}.toSet
      for {
        row: ResultRow <- apply(tp.tableName)
        partitionName <- Option(row(PARTITION_NAME))
        if partitionName.split("/").toSet == partSet
      } yield {
        row
      }
    }

  }

  /** This stores sql results indexed by table */
  class TableIndexedData(query: String) {

    val data: Map[SchemaName, Map[TableName, Seq[ResultRow]]] = {
      con.executeQuery(query)
        .groupBy{_(SCHEMA_NAME)}
        .map{case (db,rows) => (SchemaName(db), rows.groupBy{r => TableName(db,r(TABLE_NAME))})}
    }

    /** Get all rows associated to this TableName */
    def apply(tableName: TableName): Iterable[ResultRow] = {
        for {
          schemaData: Map[TableName, Seq[ResultRow]] <- data.get(tableName.schemaName).toIterable
          tableData: Seq[ResultRow] <- schemaData.get(tableName).toIterable
          row: ResultRow <- tableData
        } yield {
          row
        }
    }

    /** Get all rows associated to this TablePartitionName */
    def apply(tp: TablePartitionName): Iterable[ResultRow] = {
      val partSet: Set[String] = tp.partColNames.map{_.toString}.toSet

      for {
        schemaData: Map[TableName, Seq[ResultRow]] <- data.get(tp.schemaName).toIterable
        tableData: Seq[ResultRow] <- schemaData.get(tp.tableName).toIterable
        row: ResultRow <- tableData
        partitionName <- Option(row(PARTITION_NAME))
        if partitionName.split("/").toSet == partSet
      } yield {
        row
      }
    }
  }

  lazy val partitionsData: LazyCachedIndexedData = new LazyCachedIndexedData(partitionsQuery)

  lazy val tablesData: LazyCachedIndexedData = new LazyCachedIndexedData(tablesQuery)

  lazy val tableInfoData: LazyCachedIndexedData = new LazyCachedIndexedData(tableParametersQuery)

  lazy val columnsData: TableIndexedData = new TableIndexedData(columnsQuery)

  lazy val tableNamesData: LazyCachedIndexedData = new LazyCachedIndexedData(tableNamesQuery)

  lazy val schemasData: Map[SchemaName, ResultRow] = {
    con.executeQuery(schemasQuery)
      .map{row => (SchemaName(row(SCHEMA_NAME)),row)}
      .toMap
  }

  lazy val schemaInfoData: Map[SchemaName, ResultRow] = {
    con.executeQuery(schemaParametersQuery(""))
      .map{row => (SchemaName(row(SCHEMA_NAME)),row)}
      .toMap
  }

  override def listSchemaNames: Iterable[SchemaName] = {
    schemasData.keys.toSeq.sorted
  }

  override def listSchemasWithInfo: Iterable[SchemaWithInfo] = {
    schemaInfoData.map{
      case (schemaName, row) =>
        new SchemaWithInfo(
          None,
          row(SCHEMA_LOCATION_URI),
          schemaName,
          Option(row(NUM_TABLES)).map{_.toInt},
          Option(row(TOTAL_SIZE)).map{_.toLong},
          Option(row(NUM_FILES)).map{_.toLong},
          Option(row(MODIFICATION_TIME)).map{_.toLong * 1000}
        )
    }
  }

  override def getTableWithInfo(tableName: TableName): Option[TableWithInfo] = {
    val partitionKeys: Seq[PartitionKey] = getPartitionKeys(tableName)
    tablesData(tableName).headOption.map{
      case row: ResultRow =>
        val ioFormat = IOFormat(row(TABLE_INPUT_FORMAT), row(TABLE_OUTPUT_FORMAT), row(TABLE_SERDE))
        val info: ResultRow = tableInfoData(tableName).head
        val numParts: Option[Int] =
          if(partitionKeys.isEmpty){
            None
          }
          else {
            Option(info(NUM_PARTS)).map{_.toInt}
          }
        new TableWithInfo(
          Option(row(TABLE_CREATE_TIME)).map{_.toLong * 1000},
          row(TABLE_LOCATION),
          ioFormat,
          tableName,
          numParts,
          Option(info(TOTAL_SIZE)).map{_.toLong},
          Option(info(NUM_FILES)).map{_.toLong},
          Option(info(MODIFICATION_TIME)).map{_.toLong * 1000}
        )

    }
  }

  override def listTablesNamesInSchema(schema: SchemaName): Iterable[TableName] = {
    tableNamesData.apply(schema).map{r => TableName(schema, r(TABLE_NAME))}.toSeq.sortBy{x=>x}
  }

  override def listPartitionNames(tableName: TableName): Seq[TablePartitionName] = {
    val tablePartitionsNames: Iterable[TablePartitionName] =
      for{row: ResultRow <- partitionsData(tableName)} yield {
        TablePartitionName(tableName,row(PARTITION_NAME))
      }
    tablePartitionsNames.toSeq.sorted(TablePartitionName.order)
  }

  override def getTablePartitioningInfo(tableName: TableName): TablePartitioningInfo = {
    val partitionKeys: Seq[PartitionKey] = getPartitionKeys(tableName)
    val tablePartitions: Iterable[PartitionWithInfo] =
      if(partitionKeys.isEmpty) {
        tablesData(tableName).flatMap{
          row =>
            val ioFormat = IOFormat(row(TABLE_INPUT_FORMAT), row(TABLE_OUTPUT_FORMAT), row(TABLE_SERDE))
            val info: ResultRow = tableInfoData(tableName).head
            val p: PartitionWithEagerInfo =
              new PartitionWithEagerInfo(
                Option(row(TABLE_CREATE_TIME)).map{_.toLong * 1000},
                row(TABLE_LOCATION),
                ioFormat,
                Nil,
                Option(info(TOTAL_SIZE)).map{_.toLong},
                Option(info(NUM_FILES)).map{_.toLong},
                Option(info(MODIFICATION_TIME)).map{_.toLong * 1000}
              )
            if(p.creationTime == p.modificationTime) {
              /* If the modificationTime is the same as the creationTime, it means that the table was never populated */
              Nil
            }
            else {
              p::Nil
            }
        }
      }
      else {
        for {
          row: ResultRow <- partitionsData(tableName)
          /* If the table is partitioned but has no partition, we still get rows because of the left join, and we must drop them. */
          createTime <- Option(row(PARTITION_CREATE_TIME))
        } yield {
          val ioFormat = IOFormat(row(PARTITION_INPUT_FORMAT), row(PARTITION_OUTPUT_FORMAT), row(PARTITION_SERDE))
          val columns: Array[PartitionColumn] = row(PARTITION_NAME).split("/").map{s => val a = s.split("=") ; new PartitionColumn(a(0), Option(a(1)))}
          new PartitionWithEagerInfo(
            Some(createTime.toLong * 1000),
            row(PARTITION_LOCATION),
            ioFormat,
            columns,
            Option(row(TOTAL_SIZE)).map{_.toLong},
            Option(row(NUM_FILES)).map{_.toLong},
            Option(row(MODIFICATION_TIME)).map{_.toLong * 1000}
          )
        }
      }
    new TablePartitioningInfo(tableName, partitionKeys, tablePartitions.toSeq)
  }

  def getTableType(tableName: TableName): Option[TableType] = {
    val tableType: Iterable[TableType] =
      for{row: ResultRow <- tablesData(tableName)} yield {
        row(TABLE_TYPE) match {
          case TABLE_TYPE_VIEW     => TableType.VIEW
          case TABLE_TYPE_MANAGED  => TableType.REMOTE
          case TABLE_TYPE_EXTERNAL => TableType.REMOTE
          case _ => TableType.REMOTE
        }
      }
    tableType.headOption
  }

  override def getTable(tableName: TableName): Option[TableInfo] = {
    for {
      tableType: TableType <- getTableType(tableName)
      columns: Seq[Column] = getColumns(tableName)
      partitions: Seq[PartitionKey] = getPartitionKeys(tableName)
    } yield {
      new TableInfo(tableType, tableName, columns, partitions)
    }
  }

  def getPartitionKeys(tableName: TableName): Seq[PartitionKey] = {
    val numberedPartitions: Iterable[(Int, PartitionKey)] =
      for {
        row: ResultRow <- tablesData(tableName)
        partitionIndex <-  Option(row(PARTCOL_INDEX))
      } yield {
        (partitionIndex.toInt, new PartitionKey(row(PARTCOL_NAME), Option(row(PARTCOL_TYPE)), Option(row(PARTCOL_COMMENT))))
      }
    numberedPartitions.toSeq.sortBy{_._1}.map{_._2}
  }

  def getColumns(tableName: TableName): Seq[Column] = {
    val numberedColumns: Iterable[(Int, Column)] =
      for {
        row: ResultRow <- columnsData(tableName)
        columnIndex <- Option(row(COLUMN_INDEX))
      } yield {
        (columnIndex.toInt, new Column(row(COLUMN_NAME), row(COLUMN_TYPE), row(COLUMN_COMMENT)))
      }
    numberedColumns.toSeq.sortBy{_._1}.map{_._2}
  }

  override def getPartition(tp: TablePartitionName): Option[PartitionWithEagerInfo] = {
    val query = getPartitionQuery(tp)
    val partitions: Iterable[PartitionWithEagerInfo] =
      for(row: ResultRow <- (new TableIndexedData(query))(tp)) yield {
        val ioFormat = IOFormat(row(PARTITION_INPUT_FORMAT), row(PARTITION_OUTPUT_FORMAT), row(PARTITION_SERDE))
        val columns: Array[PartitionColumn] = row(PARTITION_NAME).split("/").map{s => val a = s.split("=") ; new PartitionColumn(a(0), Option(a(1)))}
        new PartitionWithEagerInfo(
          Option(row(PARTITION_CREATE_TIME)).map{_.toLong * 1000},
          row(PARTITION_LOCATION),
          ioFormat,
          columns,
          Option(row(TOTAL_SIZE)).map{_.toLong},
          Option(row(NUM_FILES)).map{_.toLong},
          Option(row(MODIFICATION_TIME)).map{_.toLong * 1000}
        )
      }

    assert(partitions.size <= 1, s"Only one matching partition should be found, but we found more than one: ${partitions.mkString("\n")}")

    partitions.headOption
  }

  override def close(): Unit = {
    con.close()
  }

  override def clearCache(): Unit = {
    tablesData.clearCache()
    partitionsData.clearCache()
  }

}

