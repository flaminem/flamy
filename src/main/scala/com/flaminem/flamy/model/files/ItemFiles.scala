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

import com.flaminem.flamy.model.names.ItemName
import com.flaminem.flamy.utils.logging.Logging

import scala.collection.generic.Growable
import scala.collection.mutable.ListBuffer
import scala.collection.{TraversableLike, mutable}

/**
 * A Collection of ItemFile.
 * This allow to store simply all the files associated with an item.
 */
class ItemFiles[T <: ItemFile](
  val itemName: ItemName,
  mm: mutable.MultiMap[FileType, T] = new mutable.HashMap[FileType, mutable.Set[T]] with mutable.MultiMap[FileType, T]
)
extends Traversable[T] with TraversableLike[T, ItemFiles[T]] with Growable[T] with Logging {
  // TODO: this should be immutable ?

  override protected[this] def newBuilder: mutable.Builder[T, ItemFiles[T]] = {
    new ListBuffer[T].mapResult{
      files =>
        val itemFiles = new ItemFiles[T](itemName)
        itemFiles ++= files
        itemFiles
    }
  }

  override def foreach[U](f: (T) => U): Unit = {
    mm.values.flatten.foreach{f}
  }

  def getFilesOfType(fileType: FileType): Set[T] = {
    mm.get(fileType).map{_.toSet}.getOrElse(Set())
  }

  def getFileOfType(fileType: FileType): Option[T] = {
    val set = mm.getOrElse(fileType, Set())
    if(set.size > 1) {
      logger.warn(s"The item $itemName has multiple ItemFiles associated, but only one is used.")
    }
    set.headOption
  }

  /**
    * Return a copy of this ItemFile where only the FileTypes satisfying the given predicate are kept.
    *
    * @param predicate
    * @return
    */
  def filterFileTypes(predicate: (ItemName, FileType)=>Boolean): ItemFiles[T] = {
    this.filter{
      case file => predicate(file.getItemName, file.fileType)
    }
  }

  override def toString: String = mm.values.toString

  override def +=(file: T): ItemFiles.this.type = {
    mm.addBinding(file.fileType, file)
    this
  }

  override def clear(): Unit = {
    mm.clear()
  }
}

