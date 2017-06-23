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

import java.io.File

import com.flaminem.flamy.conf.FlamyGlobalContext
import org.apache.commons.io.FileUtils
import org.scalatest.FunSuite


/**
 * Created by fpin on 5/25/15.
 */
class ActionRunner$Test extends FunSuite {

  //TODO: write a test to check that another non-StreamText subThread still works
  private val RECURSION_DEPTH: Int = 5
  private val RUN_TESTS_LOG: String = "tests/run/log"
  private val NUM_THREADS: Int = 5
  private val SLEEP_TIME_MILLIS: Int = 100

  def test(printError: Boolean, parallelize: Boolean, rec: Boolean): Unit = {
    test(printError, parallelize, rec, alternate = false, changeName = false)
  }

  def test(printError: Boolean, parallelize: Boolean, rec: Boolean, alternate: Boolean, changeName: Boolean): Unit = {
    if (rec) {
      testRec(printError, parallelize, alternate, changeName)
    }
    else {
      test(printError, parallelize)
    }
  }

  def test(printError: Boolean, parallelize: Boolean): Unit = {
    FileUtils.deleteDirectory(new File(RUN_TESTS_LOG))
    new File(RUN_TESTS_LOG).mkdir
    FlamyGlobalContext.setLogFolder(RUN_TESTS_LOG)

    var runningActions: Seq[Action] =
      for (i <- 0 until NUM_THREADS)
      yield {
        val action: Action = new PrintAction("Hello " + i, "Action " + i, printError)
        ActionRunner.run(action, parallelize)
        action
      }
    var continue = true ;
    while(continue) {
      runningActions = runningActions.filter{action => !action.isFinished}
      if (runningActions.isEmpty) {
        continue = false
      }
      else {
        Thread.sleep(SLEEP_TIME_MILLIS)
      }
    }
    check(printError)
  }

  def check(printError: Boolean): Unit = {
    var suffix: String = if (printError) "err" else "out"
    for(i <- 0 until NUM_THREADS) {
        checkFile("Action " + i + "." + suffix, "Hello " + i + "\n")
    }
    suffix = if (printError) "out" else "err"
    for(i <- 0 until NUM_THREADS) {
        checkNoFile("Action " + i + "." + suffix)
    }
  }

  private def checkFile(actionName: String, expected: String): Unit = {
    val actual: String = FileUtils.readFileToString(new File(FlamyGlobalContext.getLogFolder + "/" + actionName))
    assert(expected === actual)
  }

  private def checkNoFile(actionName: String): Unit = {
    assert(new File(FlamyGlobalContext.getLogFolder + "/" + actionName).exists() === false)
  }

  def testRec(printError: Boolean, parallelize: Boolean, alternate: Boolean, changeName: Boolean): Unit = {
    FileUtils.deleteDirectory(new File(RUN_TESTS_LOG))
    new File(RUN_TESTS_LOG).mkdir
    FlamyGlobalContext.setLogFolder(RUN_TESTS_LOG)
    var runningActions: Seq[Action] =
      for (i <- 0 until NUM_THREADS)
        yield {
          val action: Action = new RecursivePrintAction("Hello " + i, "Action " + i, printError, parallelize, alternate, changeName, RECURSION_DEPTH)
          ActionRunner.run(action, parallelize)
          action
        }
    var continue = true ;
    while(continue) {
      runningActions = runningActions.filter{action => !action.isFinished}
      if (runningActions.isEmpty) {
        continue = false ;
      }
      else {
        Thread.sleep(SLEEP_TIME_MILLIS)
      }
    }
    checkRec(printError, changeName)
  }

  def makeExpectedRec(num: Int): String = {
    val sb = new StringBuilder
    for(i <- 0 to RECURSION_DEPTH) {
        sb.append("Hello " + num + " - recursionLevel = " + i + "\n")
    }
    for(i <- RECURSION_DEPTH to 0 by -1 ) {
        sb.append("Hello " + num + " - recursionLevel = " + i + "\n")
    }
    sb.toString()
  }

  def checkRec(printError: Boolean, changeName: Boolean): Unit = {
    if (changeName) {
      for(num <- 0 until NUM_THREADS) {
        var suffix: String = if (printError) "err" else "out"
        var sub: String = ""
        for(i <- 0 until RECURSION_DEPTH) {
          val expected: String = "Hello " + num + " - recursionLevel = " + i + "\n"
          checkFile("Action " + num + sub + "." + suffix, expected + expected)
          sub += "_sub"
        }
        suffix = if (printError) "out" else "err"
        sub = ""
        for(i <- 0 until RECURSION_DEPTH) {
          checkNoFile("Action " + num + sub + "." + suffix)
          sub += "_sub"
        }
      }
    }
    else {
      var suffix: String = if (printError) "err" else "out"
      for(i <- 0 until NUM_THREADS) {
        checkFile("Action " + i + "." + suffix, makeExpectedRec(i))
      }
      suffix = if (!printError) "err" else "out"
      for(i <- 0 until NUM_THREADS) {
        checkNoFile("Action " + i + "." + suffix)
      }
    }
  }

  test("testOut") {
    test(printError = false, parallelize = false)
  }

  test("testErr") {
    test(printError = false, parallelize = false)
    test(printError = true, parallelize = false)
  }

  test("testOutParallel") {
    test(printError = false, parallelize = true)
  }

  test("testErrParallel") {
    test(printError = true, parallelize = true)
  }

  test("testRecOut") {
    test(printError = false, parallelize = false, rec = true)
  }

  test("testRecErr") {
    test(printError = true, parallelize = false, rec = true)
  }

  test("testRecOutParallel") {
    test(printError = false, parallelize = true, rec = true)
  }

  test("testRecErrParallel") {
    test(printError = true, parallelize = true, rec = true)
  }

  test("testRecOutAlternate") {
    test(printError = false, parallelize = false, rec = true, alternate = true, changeName = false)
  }

  test("testRecErrAlternate") {
    test(printError = true, parallelize = false, rec = true, alternate = true, changeName = false)
  }

  test("testRecOutParallelAlternate") {
    test(printError = false, parallelize = true, rec = true, alternate = true, changeName = false)
  }

  test("testRecErrParallelAlternate") {
    test(printError = true, parallelize = true, rec = true, alternate = true, changeName = false)
  }

  test("testRecOutChangeName") {
    test(printError = false, parallelize = false, rec = true, alternate = false, changeName = true)
  }

  test("testRecErrChangeName") {
    test(printError = true, parallelize = false, rec = true, alternate = false, changeName = true)
  }

  test("testRecOutParallelChangeName") {
    test(printError = false, parallelize = true, rec = true, alternate = false, changeName = true)
  }

  test("testRecErrParallelChangeName") {
    test(printError = true, parallelize = true, rec = true, alternate = false, changeName = true)
  }

  test("testRecOutAlternateChangeName") {
    test(printError = false, parallelize = false, rec = true, alternate = true, changeName = true)
  }

  test("testRecErrAlternateChangeName") {
    test(printError = true, parallelize = false, rec = true, alternate = true, changeName = true)
  }

  test("testRecOutParallelAlternateChangeName") {
    test(printError = false, parallelize = true, rec = true, alternate = true, changeName = true)
  }

  test("testRecErrParallelAlternateChangeName") {
    test(printError = true, parallelize = true, rec = true, alternate = true, changeName = true)
  }

  test("testAll") {
    test(printError = false, parallelize = false, rec = false, alternate = false, changeName = false)
    test(printError = true,  parallelize = false, rec = false, alternate = false, changeName = false)
    test(printError = true,  parallelize = false, rec = false, alternate = false, changeName = false)
    test(printError = false, parallelize = true,  rec = false, alternate = false, changeName = false)
    test(printError = true,  parallelize = true,  rec = false, alternate = false, changeName = false)
    test(printError = false, parallelize = false, rec = true,  alternate = false, changeName = false)
    test(printError = true,  parallelize = false, rec = true,  alternate = false, changeName = false)
    test(printError = false, parallelize = true,  rec = true,  alternate = false, changeName = false)
    test(printError = true,  parallelize = true,  rec = true,  alternate = false, changeName = false)
    test(printError = false, parallelize = false, rec = false, alternate = true,  changeName = false)
    test(printError = true,  parallelize = false, rec = false, alternate = true,  changeName = false)
    test(printError = false, parallelize = true,  rec = false, alternate = true,  changeName = false)
    test(printError = true,  parallelize = true,  rec = false, alternate = true,  changeName = false)
    test(printError = false, parallelize = false, rec = true,  alternate = true,  changeName = false)
    test(printError = true,  parallelize = false, rec = true,  alternate = true,  changeName = false)
    test(printError = false, parallelize = true,  rec = true,  alternate = true,  changeName = false)
    test(printError = true,  parallelize = true,  rec = true,  alternate = true,  changeName = false)
    test(printError = false, parallelize = false, rec = false, alternate = false, changeName = true)
    test(printError = true,  parallelize = false, rec = false, alternate = false, changeName = true)
    test(printError = false, parallelize = true,  rec = false, alternate = false, changeName = true)
    test(printError = true,  parallelize = true,  rec = false, alternate = false, changeName = true)
    test(printError = false, parallelize = false, rec = true,  alternate = false, changeName = true)
    test(printError = true,  parallelize = false, rec = true,  alternate = false, changeName = true)
    test(printError = false, parallelize = true,  rec = true,  alternate = false, changeName = true)
    test(printError = true,  parallelize = true,  rec = true,  alternate = false, changeName = true)
    test(printError = false, parallelize = false, rec = false, alternate = true,  changeName = true)
    test(printError = true,  parallelize = false, rec = false, alternate = true,  changeName = true)
    test(printError = false, parallelize = true,  rec = false, alternate = true,  changeName = true)
    test(printError = true,  parallelize = true,  rec = false, alternate = true,  changeName = true)
    test(printError = false, parallelize = false, rec = true,  alternate = true,  changeName = true)
    test(printError = true,  parallelize = false, rec = true,  alternate = true,  changeName = true)
    test(printError = false, parallelize = true,  rec = true,  alternate = true,  changeName = true)
    test(printError = true,  parallelize = true,  rec = true,  alternate = true,  changeName = true)
  }
}
