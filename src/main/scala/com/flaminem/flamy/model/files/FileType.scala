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

import com.flaminem.flamy.utils.macros.SealedValues

import scala.collection.immutable
import scala.util.matching.Regex

/**
 * Created by fpin on 7/24/15.
 */
sealed trait FileType {

  val filePrefix: String
  val fileExtension: String
  val multipleFilesAllowed: Boolean

  val commonName : String = {
    s"$filePrefix.$fileExtension"
  }

  def getFileName(title: Option[String]) : String = {
    val titleString = title.map{"_" + _}.getOrElse("")
    s"$filePrefix$titleString.$fileExtension"
  }

  /**
    * Some files may have an optional title, allowing to have multiple files of the same type for a given item.
    * For instance: a table can have multiple Populates, POPULATE.hql and POPULATE_myTitle.hql.
    */
  private[files] lazy val matchingRegex: Regex = {
    assert(filePrefix.matches("[\\p{Alnum}-_]+"))
    assert(fileExtension.matches("[\\p{Alnum}-_]+"))

    if(multipleFilesAllowed) {
      s"^$filePrefix(?:_(.+))?[.]$fileExtension$$".r
    }
    else {
      s"^$filePrefix[.]$fileExtension$$".r
    }
  }

  def matchesFileName(fileName: String): Boolean = {
    matchingRegex.findFirstMatchIn(fileName).isDefined
  }

  def getTitleFromFileName(fileName: String): Option[String] = {
    matchingRegex.findFirstMatchIn(fileName) match {
      case Some(m) if m.groupCount > 0 => Option(m.group(1))
      case _ => None
    }
  }


}

object FileType {

  case object CREATE extends FileType {
    override val filePrefix: String = "CREATE"
    override val fileExtension: String = "hql"
    override val multipleFilesAllowed: Boolean = false
  }

  case object VIEW extends FileType {
    override val filePrefix: String = "VIEW"
    override val fileExtension: String = "hql"
    override val multipleFilesAllowed: Boolean = false
  }

  case object POPULATE extends FileType {
    override val filePrefix: String = "POPULATE"
    override val fileExtension: String = "hql"
    override val multipleFilesAllowed: Boolean = true
  }

  case object TEST extends FileType {
    override val filePrefix: String = "TEST"
    override val fileExtension: String = "hql"
    override val multipleFilesAllowed: Boolean = true
  }

  case object META extends FileType {
    override val filePrefix: String = "META"
    override val fileExtension: String = "properties"
    override val multipleFilesAllowed: Boolean = false
  }

  case object CREATE_SCHEMA extends FileType {
    override val filePrefix: String = "CREATE_SCHEMA"
    override val fileExtension: String = "hql"
    override val multipleFilesAllowed: Boolean = false
  }

  case object PRESETS extends FileType {
    override val filePrefix: String = "PRESETS"
    override val fileExtension: String = "hql"
    override val multipleFilesAllowed: Boolean = false
  }

  /* This line must stay after the value declaration or it will be empty */
  val values: immutable.Seq[FileType] = SealedValues.values[FileType]

  def getTypeFromFileName(fileName: String): Option[FileType] = {
    FileType.values.find{ tpe => tpe.matchesFileName(fileName) }
  }

}
