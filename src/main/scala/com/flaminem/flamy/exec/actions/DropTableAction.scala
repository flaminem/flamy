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

package com.flaminem.flamy.exec.actions

import com.flaminem.flamy.exec.FlamyRunner
import com.flaminem.flamy.exec.utils.Action
import com.flaminem.flamy.model.TableInfo
import com.flaminem.flamy.model.names.TableName

class DropTableAction(val table: TableInfo, val flamyRunner: FlamyRunner) extends Action {

  val tableName: TableName = table.tableName
  val tableTypeString: String = {
    if(table.isView) {
      "VIEW"
    }
    else {
      "TABLE"
    }
  }

  val statement: String = s"DROP $tableTypeString IF EXISTS ${tableName.fullName}"

  override val name: String = s"DROP $tableTypeString ${tableName.fullName}"

  override val logPath: String = s"DROP_${tableTypeString}_${tableName.fullName}"

  override def run(): Unit = {
    flamyRunner.runText(statement)
  }

}
