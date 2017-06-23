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

import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.model.files.FilePath
import com.flaminem.flamy.utils.FileUtils

/**
 * Created by fpin on 5/25/15.
 */
case class ActionResult (
  returnStatus: ReturnStatus,
  logPath: String,
  exception: Option[Throwable]
) {

  private def getExceptionErrorMessage(exception: Option[Throwable]): Option[String] = {
    for(e <- exception) yield {
      val errorClass = e match {
        case _ : FlamyException => ""
        case _ => e.getClass.getName + ": "
      }
      val errorMessage = Option(e.getMessage).getOrElse("")
      s"$errorClass$errorMessage"
    }
  }

  /**
    * Search the error log file for the cause of the failure.
    *
    * @param logPath
    * @return
    */
  private def getHiveErrorMessage(logPath: String): Option[String] = {
    FileUtils.readFileToLines(new File(logPath)).filter{_.startsWith("FAILED:")} match {
      case i if i.isEmpty  => None
      case i => Some(i.next)
    }
  }

  private def errorLogPath = logPath + ".err"

  def logPathMessage: String = {
    s"    Logs available at : ${FilePath(errorLogPath)}"
  }

  def errorMessage: String = {
    val hiveMessage = getHiveErrorMessage(errorLogPath)
    val exceptionMessage = getExceptionErrorMessage(exception)
    hiveMessage.orElse(exceptionMessage).map{_ + "\n"}.getOrElse("")
  }

  /**
    * Builds an error message for this ActionResult
    *
    * @return
    */
  def fullErrorMessage: String = {
    errorMessage + logPathMessage
  }

}
