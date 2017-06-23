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

package com.flaminem.flamy.utils.io

import java.io.{FileDescriptor, FileInputStream, PrintWriter}

import com.flaminem.flamy.exec.utils.ThreadPrintStream
import com.flaminem.flamy.utils.collection.mutable.AutoResizableArray
import jline.console.ConsoleReader

import scala.math._

object MultilineWriter {
  trait Direction {
    val char: Char

    override def toString: String = char.toString
  }

  private case object Up extends Direction {
    val char = 'A'
  }
  private case object Down extends Direction {
    val char = 'B'
  }
  private case object Right extends Direction {
    val char = 'C'
  }
  private case object Left extends Direction {
    val char = 'D'
  }

  private object Cursor {
    def move(direction: Direction, steps: Int = 1): String = {
      s"\u001b[$steps$direction"
    }

    def move(pair: (Direction, Int)): String = {
      move(pair._1, pair._2)
    }
  }

  /**
    * Mixed trait that will prepend every writen line with the cursor position and line number
    * the writer thinks it is writing. Useful only for debugging
    */
  trait Debbuger extends MultilineWriter {

    override protected def moveCursor(direction: Direction, steps: Int): Unit = {
      super.moveCursor(direction, steps)
      consoleWriter.print(f"\r>$currentLine%02.0f ")
      flush()
      Thread.sleep(1000)
    }

    override protected def println(s: String, w: Int): Unit = {
      super.println(f"\r $currentLine%02.0f " + s, w)
    }

  }

  /**
    * Mixed trait that adds a line cache, so that the writer does not rewrite a line if it has already been written.
    */
  trait Memory extends MultilineWriter {

    private val lineCache = new AutoResizableArray[String]("")

    override def printLine(lineNumber: Int, text: String): Unit = {
      if(lineCache(lineNumber) != text) {
        lineCache(lineNumber) = text
        super.printLine(lineNumber, text)
      }
      else {
        ()
      }
    }
  }

  def apply(withMemory: Boolean = true, withDebug: Boolean = false): MultilineWriter = {
    (withMemory, withDebug) match {
      case (true, true) => new MultilineWriter with Memory with Debbuger
      case (true, false) => new MultilineWriter with Memory
      case (false, true) => new MultilineWriter with Debbuger
      case (false, false) => new MultilineWriter
    }
  }

}

/**
  * Utility tool to write and rewrite multiple lines on the terminal, with some built-in mechanisms to prevent
  * the text to become too ugly when resizing the terminal or such.
  *
  * If no terminal is detected, it will simply print the lines.
  *
  * Long lines of text will be written on multiple terminal rows, but it has some securities to prevent a
  * line from overwriting the lines bellow it:
  * - When a new line is added, it will take as many terminal rows as necessary to be completely displayed.
  * - When a line is rewritten, it will keep the same number of rows, and it will be truncated if it is too long to fit in the allocated space
  * - When the terminal is resized, each line will keep the same number of allocated rows.
  *
  */
class MultilineWriter extends AutoCloseable with OutputInterceptor {
  import MultilineWriter._

  val isTerminalActive: Boolean = Option(System.getenv("TERM")).isDefined

  /**
    * The line where the cursor is currently positioned.
    */
  protected var currentLine = 0

  /**
    * stores the number of rows taken by each printed line.
    */
  private val writtenLineSizes = new AutoResizableArray[Int](0)

  /**
    * stores the number of rows taken by each additional lines.
    */
  private val additionalLineSizes = new AutoResizableArray[Int](0)

  private def numLines: Int = writtenLineSizes.size

  protected val consoleReader: ConsoleReader = new ConsoleReader(new FileInputStream(FileDescriptor.in), ThreadPrintStream.systemOut)

  protected val consoleWriter: PrintWriter = new PrintWriter(consoleReader.getOutput)

  def terminalWidth: Int = consoleReader.getTerminal.getWidth

  private def length(string: String): Int = {
    jline.internal.Ansi.stripAnsi(string).length
  }

  def truncateString(s: String): String = {
    if(length(s) <= 3) {
      "..."
    }
    else {
      s.substring(0, s.length - 3) + "..."
    }
  }

  def truncateString(s: String, maxSize: Int): String = {
    if(length(s) <= maxSize) {
      s
    }
    else {
      s.substring(0, max(min(maxSize, s.length) - 3, 0)) + "..."
    }
  }

  protected def println(s: String, w: Int): Unit = {
    consoleWriter.println("\r" + s + " " * (w - length(s)))
  }

  protected def moveCursor(direction: Direction, steps: Int = 1): Unit = {
    consoleWriter.print(Cursor move (direction, steps))
  }

  private def moveUp(nbLines: Int): Unit = {
    assert(
      currentLine - nbLines >= 0,
      s"currentLine ($currentLine) - nbLines ($nbLines) >= 0"
    )
    val steps = writtenLineSizes.slice(currentLine - nbLines, currentLine).sum + additionalLineSizes.slice(currentLine - nbLines, currentLine).sum
    currentLine -= nbLines
    moveCursor(Up, steps)
  }

  private def moveDown(nbLines: Int): Unit = {
    assert(
      currentLine + nbLines <= numLines,
      s"currentLine ($currentLine) + nbLines ($nbLines) < numLines ($numLines)"
    )
    val steps = writtenLineSizes.slice(currentLine, currentLine + nbLines).sum + additionalLineSizes.slice(currentLine, currentLine + nbLines).sum
    currentLine += nbLines
    moveCursor(Down, steps)
  }

  private def moveAt(lineNumber: Int): Unit = {
    assert(lineNumber >= 0, s"lineNumber ($lineNumber) >= 0")
    lineNumber - currentLine match {
      case 0 => ()
      case d if d > 0 => moveDown(d)
      case d if d < 0 => moveUp(-d)
    }
    currentLine = lineNumber
  }

  private def moveToLastLine(): Unit = {
    moveAt(numLines)
  }

  private def printLineInTerminal(lineNumber: Int, text: String): Unit = {
    moveAt(lineNumber)
    val w: Int = terminalWidth
    val lines: Seq[String] =
      text.split("\n", -1).flatMap{
        case "" => ""::Nil
        case s => s.grouped(w)
      }.toSeq
    val newLineSize: Int = lines.size
    val oldLineSize =
      if(lineNumber >= numLines) {
        0
      }
      else {
        writtenLineSizes(lineNumber)
      }
    if(oldLineSize == 0){
      writtenLineSizes(lineNumber) = newLineSize
    }
    val adjustedLines: Seq[String] =
      if(oldLineSize > 0 && newLineSize > oldLineSize) {
        val (rtail: Seq[String], last: Seq[String]) = lines.splitAt(oldLineSize - 1)
        rtail :+ truncateString(last.head)
      }
      else {
        lines
      }
    val emptyLines: Seq[String] = (newLineSize until oldLineSize).map{_ => ""}
    (adjustedLines ++ emptyLines).foreach{println(_, w)}
    moveCursor(Up, adjustedLines.size + emptyLines.size)
    moveDown(1)
    moveToLastLine()
    flush()
  }

  private def printAdditionalLineInTerminal(text: String): Unit = {
    val w: Int = terminalWidth
    val lines: Seq[String] =
      text.split("\n", -1).flatMap{
        case "" => ""::Nil
        case s => s.grouped(w)
      }.toSeq
    val newLineSize: Int = lines.size
    if(numLines > 0){
      additionalLineSizes(numLines - 1) += newLineSize
    }
    lines.foreach{println(_, w)}
    flush()
  }

  /**
    * Write or rewrite the line specified at the given index.
    * If the specified line number is greater than the current number of line, missing lines will be added
    * with a default size of 1.
    * @param lineNumber
    * @param text
    */
  def printLine(lineNumber: Int, text: String): Unit = {
    if(isTerminalActive){
      printLineInTerminal(lineNumber, text)
    }
    else {
      consoleWriter.println(jline.internal.Ansi.stripAnsi(text))
    }
  }

  /**
    * Add additional lines bellow the currently displayed lines.
    * These lines can not be rewritten using a lineNumber.
    * @param text
    */
  def printLine(text: String): Unit = {
    if(isTerminalActive){
      printAdditionalLineInTerminal(text)
    }
    else {
      consoleWriter.println(jline.internal.Ansi.stripAnsi(text))
    }
  }

  protected def flush(): Unit = {
    consoleWriter.flush()
  }

  override def close(): Unit = {
    super.close()
  }

}

