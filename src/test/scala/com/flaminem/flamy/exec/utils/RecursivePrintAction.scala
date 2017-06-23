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

package com.flaminem.flamy.exec.utils

/**
 * Created by fpin on 5/25/15.
 */
class RecursivePrintAction private(
  message: String,
  override val name: String,
  printError: Boolean,
  parallelize: Boolean,
  alternate: Boolean,
  changeName: Boolean,
  recursionLevel: Int,
  maxLevel: Int
)
extends Action {
  def this(message: String, name: String, printError: Boolean, parallelize: Boolean, alternate: Boolean, changeName: Boolean, recursionDepth: Int) {
    this(message,name,printError,parallelize,alternate,changeName,0,Math.max(recursionDepth, 0))
  }

  override val logPath: String = name

  def printSomething(): Unit = {
    if (printError) {
      System.err.println(message + " - recursionLevel = " + recursionLevel)
      System.err.flush()
    }
    else {
      System.out.println(message + " - recursionLevel = " + recursionLevel)
      System.out.flush()
    }
  }

  @throws(classOf[InterruptedException])
  def run(): Unit = {
    printSomething()
    if (recursionLevel < maxLevel) {
      {
        val newName: String = if (changeName) name + "_sub" else name
        val action: Action =
          new RecursivePrintAction(message, newName, printError, alternate ^ parallelize, alternate, changeName, recursionLevel + 1, maxLevel)
        ActionRunner.run(action, parallelize)
        while (!action.isFinished) {
          Thread.sleep(100)
        }
      }
    }
    printSomething()
  }

}
