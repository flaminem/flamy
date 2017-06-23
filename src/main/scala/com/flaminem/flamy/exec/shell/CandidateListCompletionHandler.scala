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

import java.io.IOException
import java.util
import java.util.{Locale, ResourceBundle}

import jline.console.ConsoleReader
import jline.console.completer.CompletionHandler
import jline.internal.Ansi

import scala.collection.JavaConversions._

/**
  * This code is a java > scala translation from jline2 source code
  *
  * A {@link CompletionHandler} that deals with multiple distinct completions
  * by outputting the complete list of possibilities to the console. This
  * mimics the behavior of the
  * <a href="http://www.gnu.org/directory/readline.html">readline</a> library.
  *
  * @author <a href="mailto:mwp1@cornell.edu">Marc Prud'hommeaux</a>
  * @author <a href="mailto:jason@planet57.com">Jason Dillon</a>
  * @since 2.3
  */
object CandidateListCompletionHandler {
  @throws[IOException]
  def setBuffer(reader: ConsoleReader, value: CharSequence, offset: Int) {
    while ((reader.getCursorBuffer.cursor > offset) && reader.backspace) {
      // empty
    }
    reader.putString(value)
    reader.setCursorPosition(offset + value.length)
  }

  /**
    * Print out the candidates. If the size of the candidates is greater than the
    * {@link ConsoleReader#getAutoprintThreshold}, they prompt with a warning.
    *
    * @param candidates the list of candidates to print
    */
  @throws[IOException]
  def printCandidates(reader: ConsoleReader, candidates: util.Collection[CharSequence]) {
    val distinct = new util.HashSet[CharSequence](candidates)
    if (distinct.size > reader.getAutoprintThreshold) {
      //noinspection StringConcatenation
      reader.println()
      reader.print(Messages.DISPLAY_CANDIDATES.format(distinct.size))
      reader.flush()
      var c = 0
      val noOpt = Messages.DISPLAY_CANDIDATES_NO.format()
      val yesOpt = Messages.DISPLAY_CANDIDATES_YES.format()
      val allowed = Array(yesOpt.charAt(0), noOpt.charAt(0))
      var continue = true
      while(continue) {
        c = reader.readCharacter(allowed:_*)
        if(c == -1){
          continue = false
        }
        else {
          val tmp = new String(Array[Char](c.toChar))
          if (noOpt.startsWith(tmp)) {
            reader.println()
            continue = false
          }
          else if (yesOpt.startsWith(tmp)) {
            continue = false
          }
          else {
            reader.beep()
          }
        }
      }
    }
    // copy the values and make them distinct, without otherwise affecting the ordering. Only do it if the sizes differ.
    val newCandidates =
      if (distinct.size != candidates.size) {
        val copy = new util.ArrayList[CharSequence]
        import scala.collection.JavaConversions._
        for (next <- candidates) {
          if (!copy.contains(next)) copy.add(next)
        }
        copy
      }
      else {
        candidates
      }
    reader.println()
    reader.printColumns(candidates)
  }

  /**
    * @return true is all the elements of <i>candidates</i> start with <i>starts</i>
    */
  private def startsWith(starts: String, candidates: Seq[String]) = {
    candidates.forall{
      candidate => candidate.toLowerCase.startsWith(starts.toLowerCase)
    }
  }

  sealed trait Messages {
    import Messages._

    val name: String
    def format(args: Any *): String = {
      if(bundle == null){
        ""
      }
      else {
        String.format(bundle.getString(name), args)
      }
    }
  }

  object Messages {

    val bundle: ResourceBundle = ResourceBundle.getBundle(CandidateListCompletionHandler.getClass.getName, Locale.getDefault())

    case object DISPLAY_CANDIDATES extends Messages{
      override val name = "DISPLAY_CANDIDATES"
    }

    case object DISPLAY_CANDIDATES_YES extends Messages{
      override val name = "DISPLAY_CANDIDATES_YES"
    }

    case object DISPLAY_CANDIDATES_NO extends Messages{
      override val name = "DISPLAY_CANDIDATES_NO"
    }

  }

}

class CandidateListCompletionHandler extends CompletionHandler {
  import CandidateListCompletionHandler._

  private var printSpaceAfterFullCompletion = true
  private var stripAnsi = false

  def getPrintSpaceAfterFullCompletion: Boolean = printSpaceAfterFullCompletion

  def setPrintSpaceAfterFullCompletion(printSpaceAfterFullCompletion: Boolean) {
    this.printSpaceAfterFullCompletion = printSpaceAfterFullCompletion
  }

  def isStripAnsi: Boolean = stripAnsi

  def setStripAnsi(stripAnsi: Boolean) {
    this.stripAnsi = stripAnsi
  }

  @throws[IOException]
  def complete(reader: ConsoleReader, candidates: util.List[CharSequence], pos: Int): Boolean = {
    val buf = reader.getCursorBuffer
    // if there is only one completion, then fill in the buffer
    if (candidates.size == 1) {
      var value = Ansi.stripAnsi(candidates.get(0).toString)
      if (buf.cursor == buf.buffer.length && printSpaceAfterFullCompletion && !value.endsWith(" ")) value += " "
      // fail if the only candidate is the same as the current buffer
      if (value == buf.toString) return false
      CandidateListCompletionHandler.setBuffer(reader, value, pos)
      return true
    }
    else if (candidates.size > 1) {
      val value = getUnambiguousCompletions(candidates)
      CandidateListCompletionHandler.setBuffer(reader, value, pos)
    }
    CandidateListCompletionHandler.printCandidates(reader, candidates)
    // redraw the current console buffer
    reader.drawLine()
    true
  }

  /**
    * Returns a root that matches all the {@link String} elements of the specified {@link List},
    * or null if there are no commonalities. For example, if the list contains
    * <i>foobar</i>, <i>foobaz</i>, <i>foobuz</i>, the method will return <i>foob</i>.
    */
  //noinspection ScalaStyle
  private def getUnambiguousCompletions(candidates: util.List[CharSequence]) = {
    if (candidates == null || candidates.isEmpty) {
      null
    }
    else if (candidates.size == 1) {
      candidates.get(0).toString
    }
    else {
      // convert to an array for speed
      val first::strings =
        candidates.toList.map{
          case c if stripAnsi => Ansi.stripAnsi(c.toString)
          case c => c.toString
        }
      val candidate = new StringBuilder
      first.indices.takeWhile{
        i => startsWith(first.substring(0, i + 1), strings)
      }.foreach{
        i => candidate.append(first.charAt(i))
      }
      candidate.toString
    }
  }
}
