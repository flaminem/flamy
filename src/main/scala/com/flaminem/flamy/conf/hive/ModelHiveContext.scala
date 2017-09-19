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

package com.flaminem.flamy.conf.hive

import java.io.{File, IOException}
import java.net.{URL, URLDecoder}

import com.flaminem.flamy.commands.Drop
import com.flaminem.flamy.conf._
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.model.exceptions.FailedQueryException
import com.flaminem.flamy.utils.FileUtils
import com.flaminem.flamy.utils.logging.Logging
import com.flaminem.flamy.utils.sql.SimpleConnection
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.util.ClassUtil
import org.apache.hive.beeline.HiveSchemaTool

/**
 * This creates the required HiveContext and SessionState used for running Hive in the model environment
 */
class ModelHiveContext (
  val hiveConf: HiveConf,
  val hiveContext: Context,
  val sessionState: Option[SessionState]
) extends AutoCloseable {

  def close(): Unit = {
    hiveContext.clear()
    sessionState match {
      case None => ()
      case Some(ss) => ss.close()
    }
  }

}

object ModelHiveContext extends Logging {

  val LOCAL_METASTORE: String = "local_metastore"
  val LOCAL_WAREHOUSE: String = "local_warehouse"
  val LOCAL_DERBY_LOG: String = "derby.log"

  private val threadLocalHeavyContext: ThreadLocal[ModelHiveContext] = new ThreadLocal[ModelHiveContext]()
  private val threadLocalLightContext: ThreadLocal[ModelHiveContext] = new ThreadLocal[ModelHiveContext]()

  private var started: Boolean = false

  /* We need to change the path of the metastore and warehouse after each reset */
  private var resetCounter: Int = 0

  def localMetastore(context: FlamyContext): String = {
    FlamyGlobalContext.getUniqueRunDir + "/" + LOCAL_METASTORE + "_" + resetCounter
  }

  def localWarehouse(context: FlamyContext): String = {
    FlamyGlobalContext.getUniqueRunDir + "/" + LOCAL_WAREHOUSE + "_" + resetCounter
  }

  /**
    * For environments other than the model one, if the file "conf/$env/hive-site.xml" exists,
    * we load it into the hive configuration.
    * @param hiveConf
    * @param context
    */
  private def addEnvConf(hiveConf: HiveConf, context: FlamyContext): Unit = {
    if(context.env != Environment.MODEL_ENV) {
      val fs = context.getLocalFileSystem
      val configFiles: Seq[Path] =
        fs.listVisibleFiles(new Path(context.propertiesFileUrl.toString).getParent.suffix(s"/${context.env}"))
          .filter{_.getName.endsWith("-site.xml")}
      for {
        configFile <- configFiles
      }
        hiveConf.addResource(configFile)
    }
    else {
      hiveConf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:;create=true;databaseName=" + localMetastore(context))
    }
  }

  private def createHeavyConf(context: FlamyContext): HiveConf = {
    val hiveConf = new HiveConf(classOf[Hive])

    /* We unset this property to make sure that the user correctly sets it */
    hiveConf.unset("javax.jdo.option.ConnectionURL")

    addEnvConf(hiveConf, context)
    hiveConf.setBoolean("hive.stats.autogather", false)
    hiveConf.set("derby.stream.error.file", FlamyGlobalContext.getUniqueRunDir + "/" + LOCAL_DERBY_LOG)
    hiveConf.set("hive.metastore.warehouse.dir", localWarehouse(context))
    hiveConf.set("hive.jar.path", ClassUtil.findContainingJar(classOf[Hive]))
    hiveConf.set("mapreduce.map.log.level", "ERROR")
    hiveConf.set("mapreduce.reduce.log.level", "ERROR")
    hiveConf.setBoolean("hive.metastore.try.direct.sql", false)
    hiveConf.setBoolean("hive.metastore.fastpath", true)
    hiveConf.setBoolean("datanucleus.schema.autoCreateAll", true)
    hiveConf.setBoolean("hive.metastore.schema.verification", false)

    if(Option(hiveConf.get("javax.jdo.option.ConnectionURL")).isEmpty){
      throw new ConfigurationException(
        s"Please set the configuration property 'javax.jdo.option.ConnectionURL' in ${context.env}/hive-site.xml," +
          s" in the same directory as ${Flamy.name}'s configuration file."
      )
    }
    hiveConf
  }

  private def createLightConf(): HiveConf = {
    val hiveConf = new HiveConf()
    hiveConf.setBoolean("hive.metastore.fastpath", true)
    hiveConf.set("_hive.hdfs.session.path", "test")
    hiveConf.set("_hive.local.session.path", "test")
    hiveConf
  }

  /**
    * This creates a HiveContext without initializing a SessionState, which is much faster.
    * Use this to parse queries without having to create objects.
    *
    * @param context
    * @return
    */
  def getLightContext(context: FlamyContext): ModelHiveContext = {
    this.synchronized {
      init(context)
      Option(threadLocalLightContext.get()) match {
        case Some(modelHiveContext) => modelHiveContext
        case None =>
          logger.debug("Creating new light ModelHiveContext")
          val hiveConf = createLightConf()
          val modelHiveSessionState = new ModelHiveContext(hiveConf,  new Context(hiveConf), None)
          threadLocalLightContext.set(modelHiveSessionState)
          logger.debug("light ModelHiveContext created")
          modelHiveSessionState
      }
    }
  }
  
  def init(context: FlamyContext): Unit = {
    this.synchronized {
      if (!started) {
        logger.debug("initializing global ModelHiveSessionState")
        wipeStandaloneData(context)
        started = true
      }
    }
  }

  /**
    * This creates a full HiveContext, and initializes a SessionState
    * This takes several seconds. If you only need to parse queries, prefer the light version.
    *
    * @param context
    * @return
    */
  def getHeavyContext(context: FlamyContext): ModelHiveContext = {
    this.synchronized {
      init(context)
      Option(threadLocalHeavyContext.get()) match {
        case Some(modelHiveContext) => modelHiveContext
        case None =>
          logger.debug("Creating new heavy ModelHiveContext")
          val hiveConf: HiveConf = createHeavyConf(context)

          val ss = new CliSessionState(hiveConf)
          ss.err = System.err
          ss.out = System.out
          ss.info = System.err

          SessionState.start(ss)

          val modelHiveSessionState = new ModelHiveContext(hiveConf,  new Context(hiveConf), Some(ss))
          threadLocalHeavyContext.set(modelHiveSessionState)
          logger.debug("heavy ModelHiveContext created")
          modelHiveSessionState
      }
    }
  }


  private def removeThreadLocalContext(threadLocalContext: ThreadLocal[ModelHiveContext]): Unit = {
    Option(threadLocalContext.get()) match {
      case Some(modelHiveContext) =>
        modelHiveContext.close()
        threadLocalContext.remove()
      case None => ()
    }
  }

  /**
    * Reset the ModelHiveContext and completely wipe any existing metadata in the model environment.
    * Used only for testing.
    * (This does not seem to work)
    */
  def reset(): Unit = {
    this.synchronized {
      if(started){
        logger.debug("resetting ModelHiveContext")
        resetCounter += 1
        SessionState.detachSession()
        removeThreadLocalContext(threadLocalLightContext)
        removeThreadLocalContext(threadLocalHeavyContext)
        started = false
      }
    }
  }

  @throws(classOf[IOException])
  private def wipeStandaloneData(context: FlamyContext) {
    try {
      logger.debug("Wiping model data")
      FileUtils.deleteDirectory(new File(localMetastore(context)) )
      FileUtils.deleteDirectory(new File(localWarehouse(context)) )
    }
    catch {
      case e: IOException =>
        throw new FailedQueryException(e)
    }
  }

}
