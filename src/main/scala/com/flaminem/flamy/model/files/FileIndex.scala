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

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model.ItemFilter
import com.flaminem.flamy.model.exceptions.ItemNotFoundException
import com.flaminem.flamy.model.files.FileList._
import com.flaminem.flamy.model.names.{ItemName, SchemaName, TableName}
import com.flaminem.flamy.utils.FileUtils.listDatabaseDirectories
import com.flaminem.flamy.utils._
import com.flaminem.flamy.utils.collection.mutable.IndexedCollection

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.TreeMap
import scala.collection.mutable.ListBuffer
import scala.collection.{TraversableLike, mutable}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

sealed class Index(
  map: TreeMap[ItemName, ItemFiles[ItemFile]] = new TreeMap[ItemName, ItemFiles[ItemFile]]
)
extends IndexedCollection[ItemName, ItemFiles[ItemFile]](map)
  with TraversableLike[ItemFiles[ItemFile],Index] {

  override def newBuilder: mutable.Builder[ItemFiles[ItemFile], Index] = Index.newBuilder

  def getIndexOf(value: ItemFiles[ItemFile]): ItemName = Index.getIndexOf(value)

  def addFile(file: ItemFile): Index = {
    val name: ItemName = file.getItemName
    this.get(name) match {
      case None =>
        val itemFiles = new ItemFiles[ItemFile](name)
        itemFiles += file
        this.add(itemFiles)
      case Some(itemFiles) =>
        itemFiles += file
    }
    this
  }

}

object Index {

  def getIndexOf(value: ItemFiles[ItemFile]): ItemName = value.itemName

  def newBuilder: mutable.Builder[ItemFiles[ItemFile], Index] = {
    new ListBuffer[ItemFiles[ItemFile]].mapResult{
      files =>
        val treeMap = new TreeMap[ItemName, ItemFiles[ItemFile]]
        new Index(treeMap ++ files.map{file => getIndexOf(file) -> file})
    }
  }

  implicit val canBuildFrom: CanBuildFrom[Index, ItemFiles[ItemFile], Index] = {
    new CanBuildFrom[Index, ItemFiles[ItemFile], Index] {
      override def apply(from: Index): mutable.Builder[ItemFiles[ItemFile], Index] = {
        newBuilder
      }

      override def apply(): mutable.Builder[ItemFiles[ItemFile], Index] = {
        newBuilder
      }
    }
  }

}

class FileIndex(index: Index) {

  def filter(items: Traversable[ItemName], acceptIfEmpty: Boolean=true): FileIndex = {
    if(acceptIfEmpty && items.isEmpty){
      this
    }
    else {
      val itemFilter = ItemFilter(items, acceptIfEmpty)
      filter(itemFilter)
    }
  }

  def filter(predicate: (ItemName) => Boolean): FileIndex = {
    val newIndex: Index = index.filter{itemFiles => predicate(itemFiles.itemName)}
    new FileIndex(newIndex)
  }

  /**
    * Return a copy of this where only the FileTypes satisfying the given predicate are kept.
    *
    * @param predicate
    * @return
    */
  def filterFileTypes(predicate: (ItemName, FileType) => Boolean): FileIndex = {
    val newIndex: Index = index.map{_.filterFileTypes(predicate)}
    new FileIndex(newIndex)
  }

  def strictFilter(items: Traversable[ItemName]): Try[FileIndex] = {
    val missingItems = getMissingItems(items)
    if (missingItems.nonEmpty){
      Failure(new ItemNotFoundException("the following items were not found : " + missingItems.mkString(", ")))
    }
    else{
      Success(filter(items))
    }
  }

  def addFile(file: ItemFile): FileIndex = {
    val name: ItemName = file.getItemName
    index.get(name) match {
      case None =>
        val itemFiles = new ItemFiles[ItemFile](name)
        itemFiles += file
        index.add(itemFiles)
      case Some(itemFiles) =>
        itemFiles += file
    }
    this
  }

  def containsTable(tableName: TableName): Boolean = {
    index.contains(tableName)
  }

  def containsTable(tableFullName: String): Boolean = {
    ItemName(tableFullName) match {
      case tn : TableName => index.contains(tn)
      case default => false
    }
  }

  def getTableFiles(tableFullName: String): ItemFiles[ItemFile] = {
    index.getOrElse(tableFullName, new ItemFiles[ItemFile](tableFullName))
  }

  def getTableFilesOfType(tableName: TableName, fileType: FileType): Iterable[TableFile] = {
    for {
      itemFiles <- index.get(tableName).toIterable
      file <- itemFiles.getFilesOfType(fileType)
    } yield {
      file.asInstanceOf[TableFile]
    }
  }

  def getAllTableFilesOfType(fileType: FileType): Iterable[TableFile] = {
    assert(TableFile.VALID_FILE_TYPES.contains(fileType))
    index.getAllValues.flatMap{_.getFilesOfType(fileType)}.map{_.asInstanceOf[TableFile]}
  }

  def containsSchema(schemaName: String): Boolean = {
    ItemName(schemaName) match {
      case tn : SchemaName => index.contains(tn)
      case default => false
    }
  }

  def getSchemaFiles(schemaName: String): Option[ItemFiles[ItemFile]] = {
    index.get(schemaName)
  }

  def getSchemaFileOfType(schemaName: String, fileType: FileType): Option[SchemaFile] = {
    ItemName(schemaName) match {
      case tn : SchemaName => Some(index.get(tn).flatMap{_.getFileOfType(fileType)}.orNull.asInstanceOf[SchemaFile])
      case default => None
    }
  }

  def getAllSchemaFilesOfType(fileType: FileType): Iterable[SchemaFile] = {
    assert(SchemaFile.VALID_FILE_TYPES.contains(fileType))
    index.getAllValues.flatMap{_.getFilesOfType(fileType)}.map{_.asInstanceOf[SchemaFile]}
  }

  def getTableAndSchemaNames: scala.collection.Set[String] = {
    index.indices.map{_.fullName}
  }


  def getTableNames: Set[TableName] = {
    index.indices.flatMap {
      case s: TableName => Option(s)
      case default => None
    }
  }

  def getSchemaNames: Set[SchemaName] = {
    index.indices.flatMap{
      case s: SchemaName => Option(s)
      case default => None
    }
  }

  def getItemNames: Set[ItemName] = {
    index.indices
  }

  def getTableNamesInSchema(schema: SchemaName): Set[TableName] = {
    index.indices.flatMap{
      case t: TableName if t.isInSchema(schema) => Option(t)
      case default => None
    }
  }

  def getMissingItems[T<:TraversableLike[ItemName,T]](items: T): T = {
    items.filter{case item => !index.contains(item)}
  }

  override def toString: String = index.toString

  def isEmpty: Boolean = index.isEmpty

  def size: Int = index.size

}

object FileIndex {

  def apply(context: FlamyContext): FileIndex = {
    val index: Index = new Index()
    val dbPaths = context.modelDirs
    val files: FileList = FileUtils.listItemFiles(dbPaths)

    val schemaFiles = new mutable.HashMap[SchemaName, SchemaFile]()
    for {
      dbDir: File <- listDatabaseDirectories(dbPaths)
    } {
      val schemaFile = new MissingSchemaFile(dbDir)
      schemaFiles += schemaFile.schemaName -> schemaFile
    }

    for{
      file <- files
      fileType <- FileType.getTypeFromFileName(file.getName)
    } {
      if (TableFile.VALID_FILE_TYPES.contains(fileType)) {
        val tableFile: TableFile = new ExistingTableFile(file, fileType)
        index.addFile(tableFile)
      }
      else if (SchemaFile.VALID_FILE_TYPES.contains(fileType)) {
        val schemaFile: SchemaFile = new ExistingSchemaFile(file)
        schemaFiles += schemaFile.schemaName -> schemaFile
      }
    }
    schemaFiles.values.foreach{index.addFile}
    new FileIndex(index)
  }

}