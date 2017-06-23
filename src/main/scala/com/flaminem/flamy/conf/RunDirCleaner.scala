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

package com.flaminem.flamy.conf

import com.flaminem.flamy.utils.hadoop.{SimpleFileSystem, stringToPath}
import com.flaminem.flamy.utils.time.TimeUtils

import scala.util.Try

/**
  * Created by fpin on 12/14/15.
  */
object RunDirCleaner {

  var hasRun = false

  def cleanRunDir(context: FlamyContext): Unit = {
    this.synchronized{

      if(!hasRun){
        val fs: SimpleFileSystem = context.getLocalFileSystem
        if(fs.fileSystem.exists(FlamyGlobalContext.RUN_DIR.getProperty)) {
          for (status <- fs.fileSystem.listStatus(FlamyGlobalContext.RUN_DIR.getProperty)){
            Try(TimeUtils.fileTimeToTimeStamp(status.getPath.getName))
              .foreach{
                case time =>
                  if(TimeUtils.startTimeStamp - time > 3600000*FlamyGlobalContext.RUN_DIR_CLEANING_DELAY.getProperty) {
                    fs.remove(status.getPath, skipTrash = true)
                  }
              }
          }
        }
        hasRun = true
      }
    }
  }












}
