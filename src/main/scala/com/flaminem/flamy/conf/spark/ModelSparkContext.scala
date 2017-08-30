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

package com.flaminem.flamy.conf.spark

import java.io.File

import com.flaminem.flamy.conf.{Flamy, FlamyContext, FlamyGlobalContext}
import com.flaminem.flamy.model.files.PresetsFile
import com.flaminem.flamy.parsing.hive.QueryUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by fpin on 11/27/16.
  */
object ModelSparkContext {

  val LOCAL_METASTORE: String = "local_spark_metastore"
  val LOCAL_WAREHOUSE: String = "local_spark_warehouse"
  val LOCAL_DERBY_LOG: String = "derby.log"

  private var sparkConf: SparkConf = _

  def localMetastore(context: FlamyContext): String = {
    FlamyGlobalContext.getUniqueRunDir + "/" + LOCAL_METASTORE
  }

  def localWarehouse(context: FlamyContext): String = {
    FlamyGlobalContext.getUniqueRunDir + "/" + LOCAL_WAREHOUSE
  }


  private def init(context: FlamyContext) = {
    sparkConf = new SparkConf()
      .setAppName(Flamy.name)
      .setMaster("local")
      .set("spark.sql.crossJoin.enabled", "true")
      .set("derby.stream.error.file", FlamyGlobalContext.getUniqueRunDir + "/" + LOCAL_DERBY_LOG)
      .set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + localMetastore(context) + ";create=true")
      .set("hive.metastore.warehouse.dir", localWarehouse(context))
  }

  private lazy val _spark = {
    SparkSession.builder()
      .enableHiveSupport()
      .config(sparkConf)
      .getOrCreate()
  }

  def spark(context: FlamyContext): SparkSession = {
    init(context)
    _spark
  }

  private def addUDFJars(sqlContext: SQLContext, context: FlamyContext): Unit = {
    context.getUdfJarPaths.foreach{
      path => sqlContext.sql(s"ADD JAR $path")
    }
  }

  def runPresets(sqlContext: SQLContext, context: FlamyContext): Unit = {
    addUDFJars(sqlContext, context)
    context.HIVE_PRESETS_PATH.getProperty match {
      case Some(path) =>
        val file = new PresetsFile(new File(path))
        QueryUtils.cleanAndSplitQuery(file.text).foreach{
          query =>
            val replacedQuery = context.getVariables.replaceInText(query)
            sqlContext.sql(replacedQuery)
        }
      case _ =>
        System.out.println("No presets to run")
    }
  }


}
