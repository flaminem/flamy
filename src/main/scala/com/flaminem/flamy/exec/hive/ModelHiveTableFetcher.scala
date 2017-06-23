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
import com.flaminem.flamy.model.core.{IncompleteModel, Model}
import com.flaminem.flamy.model.files.FileIndex
import com.flaminem.flamy.model.metadata.{SchemaWithInfo, TableWithInfo}
import com.flaminem.flamy.model.names.{ItemName, SchemaName, TableName}

/**
 * Created by fpin on 2/5/15.
 */
class ModelHiveTableFetcher(context: FlamyContext, items: ItemName*) extends HiveTableFetcher {

  val fileIndex: FileIndex = context.getFileIndex.filter(items)

  /* Here, we use the fileIndex instead of the items because unknown items should not raise an error */
  lazy val preModel: IncompleteModel = Model.getIncompleteModel(context, fileIndex)

  override def listSchemaNames: Iterable[SchemaName] = fileIndex.getSchemaNames

  // TODO: implement this or create HiveTableInfoFetcher trait?
  override def listSchemasWithInfo: Iterable[SchemaWithInfo] = ???

  // TODO: implement this or create HiveTableInfoFetcher trait?
  override def getTableWithInfo(table: TableName): Option[TableWithInfo] = ???

  override def getTable(tableName: TableName): Option[TableInfo] = preModel.getTable(tableName)

  override def listTablesNamesInSchema(schema: SchemaName): Iterable[TableName] = fileIndex.getTableNamesInSchema(schema)

  override def close(): Unit = ()

}
