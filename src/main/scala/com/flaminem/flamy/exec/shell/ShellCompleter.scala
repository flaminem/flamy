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

package com.flaminem.flamy.exec.shell

import java.util

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.utils.CliUtils
import com.flaminem.flamy.utils.logging.Logging
import jline.console.completer.Completer

import scala.collection.JavaConversions._
import scala.collection.TraversableLike
import scala.collection.mutable.{Builder, ListBuffer}
import scala.util.control.NonFatal

object ShellCompleter {

  /**
    *
    * @param strings
    * @param shift number of characters to shift the cursor to the right
    */
  case class Candidates(strings: Seq[String], shift: Int = 0) extends Traversable[String] with TraversableLike[String, Candidates] {

    override def foreach[U](f: (String) => U): Unit = {
      strings.foreach(f)
    }

    override def newBuilder(): Builder[String, Candidates] = {
      new ListBuffer[String].mapResult{
        l => new Candidates(l, shift)
      }
    }
  }

}

/**
  * Created by fpin on 2/16/17.
  */
class ShellCompleter(rootContext: FlamyContext) extends Completer with Logging {

  import ShellCompleter._

  val handler: CandidateListCompletionHandler = new CandidateListCompletionHandler()

  override def complete(buffer: String, cursor: Int, candidates: util.List[CharSequence]): Int = {
    handler.setPrintSpaceAfterFullCompletion(true)
    val args: Seq[String] = CliUtils.split(buffer.substring(0, cursor))
    val lastWord: String = args.lastOption.getOrElse("")
    try {
      val truncatedArgs: Seq[String] = args.dropRight(1)
      val cliArgs = CliArgs(truncatedArgs, lastWord)
      val Candidates(strings, shift) = new OptionCompleter(handler, rootContext).complete(cliArgs)
      candidates.addAll(strings.sorted)
      cursor - cliArgs.lastWord.length + shift
    }
    catch {
      case NonFatal(e) =>
        logger.debug("Exception caught during auto-completion", e)
        cursor - lastWord.length
    }
  }

}
