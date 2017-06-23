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

package com.flaminem.flamy.exec.files

import com.flaminem.flamy.exec.utils.{Action, ActionRunner}
import com.flaminem.flamy.model.files.{FilePath, ItemFile}

/**
 * An ActionRunner that execute actions on itemFiles
 */
class FileRunner(silentOnSuccess: Boolean = false, silentOnFailure: Boolean = false) extends ActionRunner(silentOnSuccess, silentOnFailure){

  def run[T <: ItemFile, F <: (T) => Unit](fun: F, itemFiles: Iterable[T]) {
    val actions: Iterable[Action] =
      itemFiles.map{
        file =>
          new ItemFileAction(file) {
            override def run(): Unit = fun(file)
          }
      }
    this.run(actions)
  }

}
