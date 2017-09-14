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

import com.flaminem.flamy.exec.utils.Action
import com.flaminem.flamy.model.PopulateInfo
import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.utils.ordering.OrderingUtils

/**
  * An action to execute a Populate File
  */
trait PopulateAction extends Action {

  def tableName: TableName

  def populateInfo: PopulateInfo

  def hasDynamicPartitions: Boolean

}

object PopulateAction {

  /**
    * A default ordering for PopulateAction.
    * Actions with dynamicPartitions come last.
    * @tparam A
    * @return
    */
  implicit def ordering[A <: PopulateAction]: Ordering[A] = {
    new Ordering[A] {
      override def compare(x: A, y: A): Int = {
        OrderingUtils.firstNonZero(
          x.populateInfo.hasDynamicPartitions.compare(y.populateInfo.hasDynamicPartitions),
          x.populateInfo.tableName.compare(y.populateInfo.tableName),
          x.populateInfo.variables.toString.compare(y.populateInfo.variables.toString),
          x.populateInfo.tableFile.title.getOrElse("").compareTo(y.populateInfo.tableFile.title.getOrElse("")),
          Action.ordering[A].compare(x, y)
        )
      }
    }
  }

}