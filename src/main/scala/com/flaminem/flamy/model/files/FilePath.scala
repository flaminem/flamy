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

package com.flaminem.flamy.model.files



import java.io.File
import java.net.URI

import com.flaminem.flamy.conf.FlamyGlobalContext

/**
  * Created by fpin on 3/5/17.
  */
case class FilePath(path: String) {

  override def toString: String = {
    val absoluteFile = new File(path).getAbsoluteFile
    if(FlamyGlobalContext.USE_HYPERLINKS.getProperty && absoluteFile.exists()) {
      new URI("file", "", absoluteFile.getPath, null, null).toASCIIString
    }
    else {
      path
    }
  }

}
