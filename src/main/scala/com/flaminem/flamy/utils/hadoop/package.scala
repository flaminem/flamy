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

import org.apache.hadoop.fs.{FileStatus, Path}

import scala.language.implicitConversions

/**
 * Created by fpin on 12/16/14.
 */
package object hadoop {

  implicit def stringToPath(s: String): Path = new Path(s)

  implicit class FileStatusExtension(f: FileStatus) {
    def isHidden: Boolean = Seq('_','.').contains{f.getPath.getName.charAt(0)}
  }

}
