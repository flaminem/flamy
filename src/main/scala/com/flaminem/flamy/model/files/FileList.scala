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

import scala.language.implicitConversions

/**
 * Created by fpin on 10/28/14.
 */
object FileList {


  implicit def seqFileToFileList(s: Seq[File]): FileList = new FileList(s)

  def apply(filePaths: String*) : FileList = new FileList(filePaths.map(s => new File(s)))

}

class FileList(s : Seq[File]) extends Seq[File] {

  /**
   * Returns the files filtered by name (case insensitive)
   * @param names
   * @return
   */
  def filterByNameIgnoreCase(names: String*): FileList = {
    s.filter{f => names.exists{string => string.equalsIgnoreCase(f.getName)}}
  }

  override def toString(): String = f"FileList(${s.mkString(", ")})"

  override def length: Int = s.length
  override def apply(idx: Int): File = s.apply(idx)
  override def iterator: Iterator[File] = s.iterator

}

