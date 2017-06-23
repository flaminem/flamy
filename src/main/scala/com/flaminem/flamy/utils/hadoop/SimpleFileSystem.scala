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

package com.flaminem.flamy.utils.hadoop

import com.flaminem.flamy.utils.logging.Logging
import org.apache.hadoop.fs._

import scala.util.Try

/**
 * Created by fpin on 12/29/14.
 */
class SimpleFileSystem (val fileSystem: FileSystem) extends Logging {

  /**
   * Move a file/folder to trash.
   * @param path
   */
  def moveToTrash(path: Path): Boolean = {
    new Trash(fileSystem, fileSystem.getConf).moveToTrash(path) match {
      case true =>
        this.logger.info(f"Moved $path to Trash")
        true
      case false =>
        this.logger.warn(f"Failed to move $path to Trash")
        false
    }
  }

  /**
   * Move it to Trash or delete it forever if skipTrash is true.
   * @param path
   * @param skipTrash
   * @return
   */
  def remove(path: Path, skipTrash: Boolean=false): Boolean = {
    if (skipTrash) {
      if (fileSystem.delete(path,true)) {
        this.logger.info(f"Successfully deleted $path")
        true
      }
      else {
        this.logger.warn(f"Failed to delete $path")
        false
      }
    }
    else {
      if (moveToTrash(path)) {
        this.logger.info(f"Successfully moved $path to Trash")
        true
      }
      else {
        this.logger.warn(f"Failed to move $path to Trash")
        false
      }
    }
  }

  /**
   * If the given path exists, move it to Trash or delete forever if skipTrash is true.
   * @param path
   * @param skipTrash
   * @return
   */
  def removeIfExists(path: Path, skipTrash: Boolean=false): Boolean = {
    if (fileSystem.exists(path)) {
      remove(path)
    }
    else {
      this.logger.info(f"Removing $path : nothing to do, it does not exist.")
      true
    }
  }


  /**
   * Create a new directory.
   * Throw an exception if the directory already exists or if it could not be created.
   * This method should only be used internally.
   * The API should only expose more user-friendly methods (such as makeEmptyFolder)
   * @param filePath
   * @return
   */
  // TODO: test
  private[hadoop] def mkDir(filePath : Path): Boolean = {
    fileSystem.mkdirs(filePath) match {
      case true =>
        this.logger.info(s"Created new directory: $filePath")
        true
      case false =>
        this.logger.info(s"Could not create directory: $filePath")
        false
    }
  }

  /**
   * Create an empty folder at the specified path.
   * If a the folder already exists, it is send to trash and a new one is created.
   * @param filePath Path of the file to be deleted
   */
  // TODO: test
  def makeEmptyFolder(filePath: Path): Boolean =  {
    this.logger.info(s"Ensuring the folder exists and is empty: $filePath")
    if (fileSystem.exists(filePath)) {
      this.logger.info(s"The folder already exists: $filePath")
      moveToTrash(filePath) match {
        case true => mkDir(filePath)
        case false => false
      }
    } else {
      this.logger.info(s"The folder does not exist: $filePath")
      mkDir(filePath)
    }
  }

  /**
   * Gives the fully qualified path out of a path.
   * eg: for a local file system,
   * getFullyQualifiedPath("/tmp") returns "file:/tmp".
   * This method may return a Failure(java.io.FileNotFoundException)
   * @param path
   * @return
   */
  def getFullyQualifiedPath(path: Path): Try[Path] = Try(fileSystem.getFileStatus(path).getPath)

  /**
   * Recursively list all subdirectories that are non-empty and do not contain sub-directories
   * @param path
   * @return
   */
  def listNonemptyLeafDirs(path: Path): Seq[Path] = {
    val p = getFullyQualifiedPath(path).get
    fileSystem.listStatus(p).filter{!_.isHidden}.toSeq.partition{_.isDirectory} match {
      case (Nil, Nil) => Nil
      case (Nil, files) => Seq(p)
      case (dirs, _) => dirs.map{_.getPath}.flatMap{listNonemptyLeafDirs}
    }
  }

  // TODO: test
  def listVisibleFiles(path: Path, hidden: Boolean=false, recursive: Boolean=false): Seq[Path] =
    fileSystem.listStatus(path).filter{!hidden || !_.isHidden}.toSeq.partition{_.isDirectory} match {
      case (Nil,Nil)                  => Seq()
      case (Nil,files)                => files.map{_.getPath}
      case (dirs,files) if !recursive => files.map{_.getPath}
      case (dirs,files)               => dirs.map{_.getPath}.flatMap{listVisibleFiles(_,hidden,recursive)}++files.map{_.getPath}
    }

  def getContentSummary(path: Path): ContentSummary = fileSystem.getContentSummary(path)

  def getFileStatus(path: Path): FileStatus = fileSystem.getFileStatus(path)


}

