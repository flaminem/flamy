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

package com.flaminem.flamy.utils

import java.io.File

import org.apache.hadoop.fs.Path

/**
 * Created by fpin on 10/27/14.
 */
object FileUtils {

  private def isUnignoredDirectory(f: File): Boolean = {
    val prefix = f.getName.charAt(0)
    f.isDirectory && !List('.','_').contains(prefix)
  }

  private def listSubDirs(file:File) : List[File] = {
    /* We use an option here to manage null values (when no files are found) */
    Option(file.listFiles).toList.flatten.filter(isUnignoredDirectory)
  }

  private def listSubFiles(file:File) : List[File] = file.listFiles().toList.filter(!_.isDirectory)

  private def listDatabaseDirectories(f: File): Seq[File] = {
    val subDirs = listSubDirs(f)
    val (schemaDirs,otherDirs) = subDirs.partition{_.getName.endsWith(".db")}
    schemaDirs ++ otherDirs.flatMap(listDatabaseDirectories)
  }

  def listDatabaseDirectories(files: Seq[File]): Seq[File] = {
    files.flatMap{listDatabaseDirectories}
  }

  def listItemFiles(files: Seq[File]): Seq[File] = {
    val dbDirs = listDatabaseDirectories(files)
    dbDirs.flatMap{listSubDirs}.flatMap{listSubFiles} ++ dbDirs.flatMap{listSubFiles}
  }

  def readFileToString(file: File): String = {
    scala.io.Source.fromFile(file).mkString
  }

  def readFileToLines(file: File): Iterator[String] = {
    scala.io.Source.fromFile(file).getLines()
  }

  def deleteDirectory(dir: File) {
    org.apache.commons.io.FileUtils.deleteDirectory(dir)
  }

  def removeDbSuffix(s: String): String = {
    s.split("[.]db")(0).toLowerCase
  }

}
