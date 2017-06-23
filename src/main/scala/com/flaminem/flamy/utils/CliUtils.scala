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

package com.flaminem.flamy.utils

import com.flaminem.flamy.parsing.ParsingUtils

import scala.util.matching.Regex
import scala.util.matching.Regex.Groups

/**
  * Created by fpin on 2/14/17.
  */
object CliUtils {

  private val argRegex: Regex = s"""((?:[^\\s'"]+|(?:${ParsingUtils.quotedStringPattern})+)+)|\\s+\\z""".r

  private val backSlashSimpleQuoteRegex = """[\\](['\\])""".r
  private val backSlashDoubleQuoteRegex = """[\\](["\\])""".r

  private[utils] def removeBackSlashes(s: String, quote: String) = {
    quote match {
      case "'" => backSlashSimpleQuoteRegex.replaceAllIn(s, "$1")
      case "\"" => backSlashDoubleQuoteRegex.replaceAllIn(s, "$1")
    }
  }

  /**
    * Splits a string the same way bash would:
    * - split according to whitespaces
    * - ignore whitespaces inside simple or double quotes
    * - remove quotes at the first level
    * @param string
    * @return
    */
  def split(string: String): Seq[String] = {
    def replacer(m: Regex.Match): String = {
      m match {
        case Groups(quote, s) => Regex.quoteReplacement(removeBackSlashes(s, quote))
      }
    }
    for {
      Groups(arg, _, _) <- argRegex.findAllMatchIn(string).toSeq
    }
    yield {
      if(arg == null){
        ""
      }
      else {
        ParsingUtils.quotedStringRegex.replaceAllIn(arg, replacer _)
      }
    }
  }


}
