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

package com.flaminem.flamy.parsing

import com.flaminem.flamy.model._
import com.flaminem.flamy.parsing.model.{TableDependency, TableDependencyCollection}
import com.flaminem.flamy.utils.FileUtils
import org.apache.commons.configuration.{Configuration, ConfigurationException, PropertiesConfiguration}
import org.apache.hadoop.fs.Path

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

/**
 * Created by fpin on 8/1/15.
 */
object MetaParser {

  private val EQUAL_REGEX = "=".r

  private def getTableFromPath(filePath: String): Table = {
    val path: Path = new Path(filePath)
    new Table(TableType.EXT, path.getParent.getName, FileUtils.removeDbSuffix(path.getParent.getParent.getName))
  }

  @throws(classOf[ConfigurationException])
  def parseTableDependencies(filePath: String): TableDependency = {
    val table: TableDependency = new TableDependency(getTableFromPath(filePath))
    val deps: TableDependencyCollection = table.tableDeps
    val conf: Configuration = new PropertiesConfiguration(filePath)
    val dependencies: Seq[String] = conf.getList("dependencies").map{_.asInstanceOf[String]}
    for (dep <- dependencies) {
      val a: Array[String] = dep.split("[.]")
      if (a.length > 1) {
        deps.add(new TableDependency(TableType.EXT, a(1), a(0)))
      }
      else {
        deps.add(new TableDependency(TableType.EXT, a(0)))
      }
    }
    table
  }

  @throws(classOf[ConfigurationException])
  def parseVariables(filePath: String): Variables = {
    try{
      unsafeParseVariables(filePath)
    }
    catch {
      case NonFatal(e) =>
        throw new ConfigurationException(s"Error found when parsing variables file $filePath", e)
    }
  }

  private def unsafeParseVariables(filePath: String): Variables = {
    val res: Variables = new Variables
    for {
      (line, lineNumber) <- scala.io.Source.fromFile(filePath, "UTF-8").getLines().map{_.trim}.zipWithIndex
      if line.length > 0 && line(0) != '#'
    } {
      if (!line.contains('=')) {
        throw new ConfigurationException(
          s"""No '=' sign found (at line $lineNumber) :
              |  $line
           """.stripMargin
        )
      }
      else {
        val Array(key, value) = line.split("=", 2)
        res.put(key.trim, value.trim)
      }
    }
    res
  }


}
