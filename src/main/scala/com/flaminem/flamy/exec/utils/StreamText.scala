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

import java.io.{File, PrintStream}

import com.flaminem.flamy.utils.io.ConcurrentFilePrintStream
import com.flaminem.flamy.utils.logging.Logging
import org.apache.hadoop.fs.FileSystem

/**
  * Courtesy from: http://maiaco.com/articles/java/threadOut.php
  * A class that sets System.out for the currently executing thread to a text file.
  */
class StreamText (
  val action: Action,
  val fs: FileSystem
) extends Runnable with Logging {

  /* Useful for debugging */
  private lazy val recursionLevel: Int = getRecursionLevel

  /**
   * Analyze the stacktrace to know if we are inside a recursive call
   * @return
   */
  private def getRecursionLevel: Int = {
    val stackTrace: Seq[StackTraceElement] = Thread.currentThread.getStackTrace.toSeq

    stackTrace.count{
      case element => element.getClassName == this.getClass.getCanonicalName && element.getMethodName == "run"
    }
  }

  private def openStreams(logPath: String) {
    openStream(logPath + ".out", System.out)
    openStream(logPath + ".err", System.err)
  }

  private def openStream(logPath: String, stream: PrintStream) {
    stream synchronized {
      val printStream: PrintStream = new ConcurrentFilePrintStream(new File(logPath), fs)
      stream.asInstanceOf[ThreadPrintStream].setThreadOut(printStream)
    }
  }

  def run() {
    val parentOut: PrintStream = System.out.asInstanceOf[ThreadPrintStream].getThreadOut
    val parentErr: PrintStream = System.err.asInstanceOf[ThreadPrintStream].getThreadOut
    var open: Boolean = false
    try {
      /*
        Create a text file where System.out.println() will send its data for this thread.
      */
      openStreams(action.fullLogPath)
      open = true
      action.execute()
    }
    finally {
      /*
        If the streams were successfully opened,
        close System.out for this thread which will flush and close this thread's text file.
      */
      if (open) {
        System.out.flush()
        System.err.flush()
        System.out.close()
        System.err.close()
      }
      /*
        Restore System.out to previous value.
      */
      System.out.asInstanceOf[ThreadPrintStream].setThreadOut(parentOut)
      System.err.asInstanceOf[ThreadPrintStream].setThreadOut(parentErr)
    }
  }
}
