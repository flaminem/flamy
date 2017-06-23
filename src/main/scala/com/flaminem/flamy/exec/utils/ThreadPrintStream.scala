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

import java.io.{ByteArrayOutputStream, PrintStream}

/**
 * Courtesy from: http://maiaco.com/articles/java/threadOut.php
 * A ThreadPrintStream replaces the normal System.out and ensures that output to System.out goes to a different PrintStream for each thread. It does this by
 * using ThreadLocal to maintain a PrintStream for each thread.
 */
class ThreadPrintStream extends PrintStream(new ByteArrayOutputStream(0)) {

  import ThreadPrintStream._

  /** Thread specific storage to hold a PrintStream for each thread */
  private val _out: InheritableThreadLocal[PrintStream] = new InheritableThreadLocal[PrintStream]

  /** Sets the PrintStream for the currently executing thread. */
  def setThreadOut(out: PrintStream) {
    if (out == null) {
      throw new RuntimeException("out is null!")
    }
    this._out.set(out)
  }

  /** Returns the PrintStream for the currently executing thread. */
  def getThreadOut: PrintStream = {
    if (this._out == null || this._out.get == null) {
      systemOut
    }
    else {
      this._out.get
    }
  }

  override def checkError: Boolean = getThreadOut.checkError

  override def println(): Unit = {
    getThreadOut.println()
  }

  override def println(x: Boolean): Unit = getThreadOut.println(x)

  override def println(x: Char): Unit = getThreadOut.println(x)

  override def println(x: Int): Unit = getThreadOut.println(x)

  override def println(x: Long): Unit = getThreadOut.println(x)

  override def println(x: Float): Unit = getThreadOut.println(x)

  override def println(x: Double): Unit = getThreadOut.println(x)

  override def println(s: String): Unit = getThreadOut.println(s)

  override def println(s: Array[Char]): Unit = getThreadOut.println(s)

  override def println(s: Any): Unit = getThreadOut.println(s)

  override def write(buf: Array[Byte], off: Int, len: Int): Unit = getThreadOut.write(buf, off, len)

  override def write(b: Int): Unit = getThreadOut.write(b)

  override def flush(): Unit = getThreadOut.flush()

  override def close(): Unit = this._out.get.close()

  override def toString: String = "ThreadPrintStream [out=" + this._out.get + "]"

}

object ThreadPrintStream {

  val systemOut: PrintStream = System.out
  val systemErr: PrintStream = System.err

  private var _out: PrintStream = System.out
  private var _err: PrintStream = System.err

  def out: PrintStream = _out
  def err: PrintStream = _err

  def setOut(newOut: PrintStream): Unit = {
    _out = newOut
  }
  def setErr(newErr: PrintStream): Unit = {
    _err = newErr
  }
  def restoreOut(): Unit = {
    _out = systemOut
  }
  def restoreErr(): Unit = {
    _err= systemErr
  }

  @volatile
  private var initialized: Boolean = false

  def init(): Unit = {
    this.synchronized {
      if(!initialized) {
        initialized = true
        replaceSystemOut()
        replaceSystemErr()
      }
    }
  }

  def shutdown(): Unit = {
    this.synchronized {
      if(initialized){
        initialized = false
        restoreSystemOut()
        restoreSystemErr()
      }
    }
  }

  /**
   * Changes System.out to a ThreadPrintStream which will send output to a separate file for each thread.
   */
  def replaceSystemOut() {
    val threadOut: ThreadPrintStream = new ThreadPrintStream
    threadOut.setThreadOut(_out)
    System.setOut(threadOut)
    Console.setOut(threadOut)
  }

  /**
   * Changes System.err to a ThreadPrintStream which will send output to a separate file for each thread.
   */
  def replaceSystemErr() {
    val threadErr: ThreadPrintStream = new ThreadPrintStream
    threadErr.setThreadOut(_err)
    System.setErr(threadErr)
    Console.setErr(threadErr)
  }


  /**
    * Restores System.out to its previous value.
    */
  def restoreSystemOut() {
    System.out.flush()
    System.setOut(systemOut)
    Console.setOut(systemOut)
  }

  /**
    * Restores System.err to its previous value.
    */
  def restoreSystemErr() {
    System.err.flush()
    System.setErr(systemErr)
    Console.setErr(systemErr)
  }
}