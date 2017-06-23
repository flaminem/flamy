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

package com.flaminem.flamy.model.files

import java.io.File

import com.flaminem.flamy.model.names.{ItemName, SchemaName, TableName}
import com.flaminem.flamy.utils.FileUtils
import org.apache.hadoop.fs.Path

/**
 * Created by fpin on 10/27/14.
 */
trait ItemFile {

  val fileType: FileType

  def text: String
  def path: Path

  def filePath: String = FilePath(path.toString).toString

  def fileName: String

  def title: Option[String] = fileType.getTitleFromFileName(fileName)

  def titleSuffix: String = title.map{" (" + _ + ")"}.getOrElse("")

  def getItemName: ItemName
  def getRelativePath: String

  override def toString: String = fileType.toString
}

trait ExistingItemFile extends ItemFile {
  val file: File

  lazy val text: String = FileUtils.readFileToString(file)
  def path: Path = new Path(file.getPath)
  def fileName: String = file.getName
}


object SchemaFile {

  final val VALID_FILE_TYPES: List[FileType] = List(FileType.CREATE_SCHEMA)

}

trait SchemaFile extends ItemFile {

  def schemaName: SchemaName

  override def getItemName: ItemName = schemaName

  override def getRelativePath: String = {
    val schema: Path = path.getParent
    //noinspection ScalaStyle
    if (schema != null) {
      val schemaName: String = schema.getName.toLowerCase
      schemaName + "/" + fileName
    }
    else {
      fileName
    }
  }

}

class ExistingSchemaFile(override val file: File) extends SchemaFile with ExistingItemFile {

  override val fileType: FileType = FileType.getTypeFromFileName(file.getName).get

  if (!SchemaFile.VALID_FILE_TYPES.contains(fileType)) {
    throw new IllegalArgumentException("This file type is not supported : " + file.getName)
  }

  override val schemaName: SchemaName = SchemaName(FileUtils.removeDbSuffix(path.getParent.getName))

}


class MissingSchemaFile(dir: File) extends SchemaFile {

  val schemaName = SchemaName(FileUtils.removeDbSuffix(dir.getName))

  override val fileType: FileType = FileType.CREATE_SCHEMA

  override def text: String = s"CREATE SCHEMA IF NOT EXISTS $schemaName"

  override def path: Path = new Path(dir.getPath)

  override def fileName: String = s"$path/${fileType.commonName}"

}


trait TableFile extends ItemFile {

  val tableName: TableName

  override def getItemName: ItemName = tableName

  /**
    * For schema.db/table/POPULATE.hql
    * > schema.table
    *
    * For schema.db/table/POPULATE_title.hql
    * > schema.table (title)
    * @return
    */
  def tableFileName: String = s"$tableName$titleSuffix"

  override def getRelativePath: String = {
    val table: Path = path.getParent
    //noinspection ScalaStyle
    if (table != null) {
      val tableName: String = table.getName.toLowerCase
      val schema: Path = table.getParent
      //noinspection ScalaStyle
      if (schema != null && schema.getName.contains(".")) {
        schema.getName.toLowerCase + "/" + tableName + "/" + fileName
      }
      else {
        tableName + "/" + fileName
      }
    }
    else {
      fileName
    }
  }

}

class ExistingTableFile(override val file: File, override val fileType: FileType) extends TableFile with ExistingItemFile {

  if (!TableFile.VALID_FILE_TYPES.contains(fileType)) {
    throw new IllegalArgumentException("This file type is not supported : " + file)
  }

  val tableName: TableName = TableName(FileUtils.removeDbSuffix(path.getParent.getParent.getName) + "." + path.getParent.getName)

}

object TableFile {
  final val VALID_FILE_TYPES: List[FileType] = List(FileType.CREATE, FileType.META, FileType.POPULATE, FileType.VIEW, FileType.TEST)
}

class PresetsFile(override val file: File) extends ExistingItemFile {

  val fileType: FileType = FileType.PRESETS

  override def getItemName: ItemName = ItemName("!presets")

  override def getRelativePath: String = "presets"

}

