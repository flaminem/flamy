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

import java.io._

import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.fusesource.jansi.AnsiOutputStream

import scala.collection.mutable

/**
 * A FileStream that can be used by multiple concurrent threads.
 * If it is closed and a thread tries to write on it, it will be automatically reopened.
 */
class ConcurrentFilePrintStream(file: File, fs: FileSystem, append: Boolean) extends PrintStream(new ByteArrayOutputStream(0)) {

  import ConcurrentFilePrintStream._

  private val stream: AutoOpeningFileOutputStream = {
    map.synchronized{
      if (map.contains(file)) {
        map(file)
      }
      else {
        val s = new AutoOpeningFileOutputStream(file, fs, append)
        map.put(file, s)
        s
      }
    }
  }

  @throws(classOf[IOException])
  def this(file: File, fs: FileSystem) = this(file, fs, false)

  override def checkError: Boolean = stream.checkError

  override def write(buf: Array[Byte], off: Int, len: Int): Unit = {
    stream.write(buf, off, len)
  }

  override def write(b: Int): Unit = stream.write(b)

  override def flush(): Unit = stream.flush()

  override def close(): Unit = stream.close()

  override def toString: String = "ConcurrentFilePrintStream [stream=" + stream + "]"

}

object ConcurrentFilePrintStream {

  private val map: mutable.HashMap[File, AutoOpeningFileOutputStream] = new mutable.HashMap[File, AutoOpeningFileOutputStream]

  /**
    * A FileOutpuStream that will automatically open itself when being used.
    * It must be closed manually, but will automatically reopen if used again.
    * Only one stream per file can be opened, though.
    *
    * The creation of the file is lazy: if the stream is created but is not used, no file will be created.
    * ANSI escape codes are filtered out when writing in the file.
    * @param file
    * @param fs
    * @param append
    */
  private class AutoOpeningFileOutputStream(file: File, fs: FileSystem, append: Boolean) extends PrintStream(new ByteArrayOutputStream(0)) {

    private var isOpen: Boolean = false

    private var stream: PrintStream = null

    private var initialized: Boolean = false

    val path: Path = new Path(file.toURI)

    def init(): Unit = {
      this.synchronized {
        if(!initialized) {
          if (!fs.exists(path.getParent)) {
            fs.mkdirs(path.getParent, FsPermission.getDirDefault)
          }
          if (!append) {
            fs.delete(path, false)
          }
          initialized = true
        }
      }
    }

    def this(file: File, fs: FileSystem) {
      this(file, fs, false)
    }

    @throws(classOf[FileNotFoundException])
    private def open(): Unit = {
      this.synchronized {
        if (!isOpen) {
          init()
          this.stream = new PrintStream(new AnsiOutputStream(new FileOutputStream(file, true)))
          this.isOpen = true
        }
      }
    }

    private def tryOpen(): Boolean = {
      try {
        open()
        true
      }
      catch {
        case e: FileNotFoundException =>
        false
      }
    }

    override def checkError: Boolean = {
      if (tryOpen()) {
        stream.checkError
      }
      else {
        true
      }
    }

    override def write(buf: Array[Byte], off: Int, len: Int): Unit =  {
      this.synchronized {
        if (tryOpen()) {
          stream.write(buf, off, len)
        }
      }
    }

    override def write(b: Int): Unit = {
      this.synchronized {
        if (tryOpen()) {
          stream.write(b)
        }
      }
    }

    override def flush(): Unit = {
      this.synchronized {
        if (isOpen) {
          stream.flush()
        }
      }
    }

    override def close(): Unit = {
      this.synchronized {
        if (isOpen) {
          stream.close()
          isOpen = false
        }
      }
    }

    override def toString: String = "MultiFileOutputStream [isOpen=" + isOpen + ", file=" + file + "]"

  }

}
