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

package com.flaminem.flamy.parsing.hive

import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.utils.TextUtils

import scala.util.matching.Regex

/**
 * Created by fpin on 11/20/14.
 */
class FlamyParsingException(
  val query: Option[String],
  message: Option[String],
  cause: Option[Throwable]
) extends FlamyException(message, cause) {

  def this(message: String) {
    this(None, Option(message),None)
  }

  def this(cause: Throwable) {
    this(None, Option(cause.getMessage),Option(cause))
  }

  def this(message: String, cause: Throwable) {
    this(None, Option(message),Option(cause))
  }

}

object FlamyParsingException {
  private val lineNumPattern: Regex = "^line (\\d+):(\\d+) .*".r

  @throws(classOf[FlamyException])
  def apply(query: String, e: Throwable, verbose: Boolean): FlamyParsingException = {
    val message: String = e.getMessage
    if (verbose) {
      FlamyOutput.err.error("Error parsing query:\n\n" + query + "\n\n" + Option(message).map{_ + "\n"}.getOrElse(""))
    }
    val querySample: Option[String] =
      if(Option(message).isDefined){
        lineNumPattern.findFirstMatchIn(message) match {
          case Some(matcher) =>
            val lineNumber: Int = matcher.group(1).toInt
            val charNumber: Int = matcher.group(2).toInt
            Some(
              message +
              "\n-   " + TextUtils.getLine(query, lineNumber - 1) +
              "\n>>> " + TextUtils.getLine(query, lineNumber) +
              "\n-   " + TextUtils.getLine(query, lineNumber + 1)
            )
          case None => Some(message)
        }
      }
      else {
        None
      }
    new FlamyParsingException(Some(query), querySample, Some(e))
  }


}