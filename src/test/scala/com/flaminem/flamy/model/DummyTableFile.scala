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

package com.flaminem.flamy.model

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model.files.{FileType, TableFile}
import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.parsing.hive.{CreateTableParser, PopulatePreParser}
import org.apache.hadoop.fs.Path

/**
  * Created by fpin on 7/14/16.
  */
//noinspection ScalaStyle
class DummyTableFile(override val tableName: TableName, _text: String) extends TableFile {

  override lazy val text: String = _text

  override def title: Option[String] = None

  override val fileType: FileType = FileType.POPULATE

  override def path: Path = new Path("")

  override def fileName: String = ""
}

object DummyTableFile {

  def apply(text: String)(implicit flamyContext: FlamyContext): DummyTableFile = {
    val tableName =
      text.trim.split("\\s+", 3).take(2) match {
        case Array(w1, w2) if w1.equalsIgnoreCase("CREATE") && w2.equalsIgnoreCase("TABLE") =>
          CreateTableParser.parseText(text).fullName
        case _ =>
          PopulatePreParser.parseText(text).head.fullName
      }
    new DummyTableFile(tableName, text)
  }

}
