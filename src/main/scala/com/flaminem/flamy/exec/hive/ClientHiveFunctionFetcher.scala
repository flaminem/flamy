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

import com.flaminem.flamy.conf.FlamyContext
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal

/**
  * Created by fpin on 11/21/16.
  */
class ClientHiveFunctionFetcher(override val context: FlamyContext) extends ModelHiveFunctionFetcher(context){

  val conf: HiveConf = new HiveConf
  val hiveMetastoreUri: String = context.HIVE_METASTORE_URI.getProperty
  conf.set("hive.metastore.uris", hiveMetastoreUri)
  val cli: HiveMetaStoreClient = new HiveMetaStoreClient(conf)

  val cache = mutable.HashMap[String, String]()

  override def getFunctionClassName(functionName: String): Try[String] = {
    super.getFunctionClassName(functionName).recover{
      case NonFatal(e) => cache.getOrElseUpdate(functionName, cli.getFunction("default", functionName).getClassName)
    }
  }

}
