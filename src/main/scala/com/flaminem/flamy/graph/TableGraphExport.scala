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

package com.flaminem.flamy.graph

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.graph.export.{TableGraphFullExporter, TableGraphLightExporter, TableGraphSchemaExporter}
import com.flaminem.flamy.model.core.Model

/**
  * A wrapper for the export methods on a TableGraph
  */
class TableGraphExport(g: TableGraph) {

  def toLightPng(path: String): Unit = new TableGraphLightExporter(g).exportToPng(path)
  def toFullPng(path: String): Unit = new TableGraphFullExporter(g).exportToPng(path)
  def toFullSvg(path: String): Unit = new TableGraphFullExporter(g).exportToSvg(path)
  def toSchemaPng(path: String): Unit = new TableGraphSchemaExporter(g).exportToPng(path)
  def toLightDot: String = new TableGraphLightExporter(g).toDot
  def toFullDot: String = new TableGraphFullExporter(g).toDot

}
