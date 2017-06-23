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

import java.io.{ByteArrayOutputStream, PrintStream}

import com.flaminem.flamy.exec.utils.ThreadPrintStream

/**
  * Created by fpin on 2/19/17.
  */
trait OutputInterceptor extends AutoCloseable {
  import OutputInterceptor._

  def printLine(s: String): Unit

  val interceptorOut: InterceptorPrintStream = new InterceptorPrintStream(printLine)
  val interceptorErr: InterceptorPrintStream = new InterceptorPrintStream(printLine)

  ThreadPrintStream.setOut(interceptorOut)
  ThreadPrintStream.setErr(interceptorErr)

//  val systemOut: PrintStream = {
//    System.out match {
//      case s: ThreadPrintStream =>
//        val sout = s.getThreadOut
//        s.setThreadOut(interceptorOut)
//        sout
//      case _ =>
//        val sout = System.out
//        System.setOut(interceptorOut)
//        Console.setOut(interceptorOut)
//        sout
//    }
//  }
//
//  val systemErr: PrintStream = {
//    System.err match {
//      case s: ThreadPrintStream =>
//        val serr = s.getThreadOut
//        s.setThreadOut(interceptorErr)
//        serr
//      case _ =>
//        val serr = System.err
//        System.setOut(interceptorErr)
//        Console.setOut(interceptorErr)
//        serr
//    }
//  }

  override def close(): Unit = {
    ThreadPrintStream.restoreOut()
    ThreadPrintStream.restoreErr()
//    System.out match {
//      case s: ThreadPrintStream =>
//        s.setThreadOut(systemOut)
//      case _ =>
//        System.setOut(systemOut)
//        Console.setOut(systemOut)
//    }
//    System.err match {
//      case s: ThreadPrintStream =>
//        s.setThreadOut(systemErr)
//      case _ =>
//        System.setErr(systemErr)
//        Console.setErr(systemErr)
//    }
    interceptorOut.close()
    interceptorErr.close()
  }

}


object OutputInterceptor {

  class InterceptorPrintStream(printLine: (String) => Unit) extends PrintStream(new ByteArrayOutputStream(), true) {

    val buf: ByteArrayOutputStream = this.out.asInstanceOf[ByteArrayOutputStream]

    override def println(): Unit = {
      printLine("")
    }

    override def println(x: Boolean): Unit = {
      printLine(x.toString)
    }

    override def println(x: Char): Unit = {
      printLine(x.toString)
    }

    override def println(x: Int): Unit = {
      printLine(x.toString)
    }

    override def println(x: Long): Unit = {
      printLine(x.toString)
    }

    override def println(x: Float): Unit = {
      printLine(x.toString)
    }

    override def println(x: Double): Unit = {
      printLine(x.toString)
    }

    override def println(s: String): Unit = {
      printLine(s)
    }

    override def println(s: Array[Char]): Unit = {
      printLine(new String(s))
    }

    override def println(s: Any): Unit = {
      printLine(s.toString)
    }

    override def flush(): Unit = {
      if(buf.size() > 0){
        printLine("<" + this.out.toString)
      }
      buf.reset()
    }

    override def toString: String = "InterceptorPrintStream"

  }

}
