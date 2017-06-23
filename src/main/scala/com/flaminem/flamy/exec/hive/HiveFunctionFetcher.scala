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

package com.flaminem.flamy.exec.hive

import com.flaminem.flamy.conf.{FlamyConfVars, FlamyContext}
import com.flaminem.flamy.model.exceptions.FlamyException
import org.apache.hadoop.hive.ql.exec.FunctionUtils
import org.apache.hadoop.hive.ql.exec.FunctionUtils.UDFClassType

import scala.util.Try

/**
  * Created by fpin on 11/21/16.
  */
trait HiveFunctionFetcher {

  def context: FlamyContext

  def getFunctionClassName(functionName: String): Try[String]

  def isUDAF(functionName: String): Boolean = {
    val className =
      getFunctionClassName(functionName).getOrElse{
        throw new FlamyException(s"Unknown function $functionName")
      }

    val classType = FunctionUtils.getUDFClassType(Class.forName(className, true, context.getUdfClassLoader))
    classType == UDFClassType.GENERIC_UDAF_RESOLVER || classType == UDFClassType.UDAF
  }


}

object HiveFunctionFetcher {

  def apply(context: FlamyContext): ModelHiveFunctionFetcher = {
    if(context.env == FlamyConfVars.MODEL_ENV) {
      new ModelHiveFunctionFetcher(context)
    }
    else{
      new ClientHiveFunctionFetcher(context)
    }
  }

}