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

import org.apache.commons.lang3.SystemUtils

/**
  * Created by fpin on 3/5/17.
  */
object SystemContext {

  trait OSFamily {

    /**
      * The name of the command to automatically open files with the correct application, on this OS Family.
      * @return
      */
    def openCommand: String

    /**
      * This flag indicates if the openCommand supports opening multiple files at once.
      * This is required for Mac OS X, as the image viewer does not allow to switch between images unless they were opened together.
      * @return
      */
    def isMultiOpenSuported: Boolean

  }

  object OSFamily {
    object Windows extends OSFamily {
      override val openCommand: String = "start"
      override val isMultiOpenSuported: Boolean = false
    }

    object Mac extends OSFamily {
      override val openCommand: String = "open"
      override val isMultiOpenSuported: Boolean = true
    }

    object Linux extends OSFamily {
      override val openCommand: String = "xdg-open"
      override val isMultiOpenSuported: Boolean = false
    }

    object Other extends OSFamily {
      override val openCommand: String = "xdg-open"
      override val isMultiOpenSuported: Boolean = false
    }
  }

  def osFamily: OSFamily = {
    true match {
      case _ if SystemUtils.IS_OS_WINDOWS => OSFamily.Windows
      case _ if SystemUtils.IS_OS_MAC => OSFamily.Mac
      case _ if SystemUtils.IS_OS_LINUX => OSFamily.Linux
      case _ if SystemUtils.IS_OS_UNIX => OSFamily.Other
    }
  }

  val userName: String = System.getProperty("user.name")

  val userHome: String = System.getProperty("user.home")

}
