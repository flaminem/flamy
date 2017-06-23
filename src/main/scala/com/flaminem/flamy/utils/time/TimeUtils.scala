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

package com.flaminem.flamy.utils.time

import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Calendar, TimeZone}

/**
  * Contains utility functions to manipulate time.
  * Can be used concurrently (thread-safe).
  */
object TimeUtils {

  private val threadLocalTimeUtils: ThreadLocal[TimeUtils] =
    new ThreadLocal[TimeUtils]{
      override def initialValue() = new TimeUtils()
    }

  private def timeUtils: TimeUtils = threadLocalTimeUtils.get()

  /** Starting time of the JVM, in millisecond */
  val startTimeStamp: Long = nowToTimeStamp()

  /** Starting time of the JVM, in universal time format */
  val startUniversalTime: String = nowToUniversalTime()

  /** Starting time of the JVM, in file time format */
  val startFileTime: String = nowToFileTime()

  def timestampToUniversalTime(timestamp: Long): String = {
    timeUtils.timestampToUniversalTime(timestamp)
  }

  def universalTimeToTimeStamp(time: String): Long = {
    timeUtils.universalTimeToTimeStamp(time)
  }

  def timestampToFileTime(timestamp: Long): String = {
    timeUtils.timestampToFileTime(timestamp)
  }

  def fileTimeToTimeStamp(time: String): Long= {
   timeUtils.fileTimeToTimeStamp(time)
  }

  def nowToTimeStamp(): Long= {
    timeUtils.nowToTimeStamp()
  }

  def nowToUniversalTime(): String= {
    timeUtils.nowToUniversalTime()
  }

  def nowToFileTime(): String= {
    timeUtils.nowToFileTime()
  }

}

private class TimeUtils {

  val calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))

  val universalTimeFormat: DateFormat = {
    val f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    f.setCalendar(calendar)
    f
  }

  /** A special time format, because space in file names are not handled well by some tools. */
  val fileTimeFormat: DateFormat = {
    val f = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss")
    f.setCalendar(calendar)
    f
  }

  def timestampToUniversalTime(timestamp: Long): String = {
    calendar.setTimeInMillis(timestamp)
    universalTimeFormat.format(calendar.getTime)
  }

  def universalTimeToTimeStamp(time: String): Long = {
    calendar.setTime(universalTimeFormat.parse(time))
    calendar.getTimeInMillis
  }

  def timestampToFileTime(timestamp: Long): String = {
    calendar.setTimeInMillis(timestamp)
    fileTimeFormat.format(calendar.getTime)
  }

  def fileTimeToTimeStamp(time: String): Long= {
    calendar.setTime(fileTimeFormat.parse(time))
    calendar.getTimeInMillis
  }

  def nowToTimeStamp(): Long= {
    System.currentTimeMillis()
  }

  def nowToUniversalTime(): String= {
    calendar.setTimeInMillis(System.currentTimeMillis())
    universalTimeFormat.format(calendar.getTime)
  }

  def nowToFileTime(): String= {
    calendar.setTimeInMillis(System.currentTimeMillis())
    fileTimeFormat.format(calendar.getTime)
  }







}