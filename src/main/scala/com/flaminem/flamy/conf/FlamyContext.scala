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

import java.io.File
import java.lang.management.ManagementFactory
import java.net.{MalformedURLException, URI, URL, URLClassLoader}

import com.flaminem.flamy.exec.hive.{HiveFunctionFetcher, ModelHiveFunctionFetcher}
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.model.Variables
import com.flaminem.flamy.model.files.FileIndex
import com.flaminem.flamy.parsing.MetaParser
import com.flaminem.flamy.utils.hadoop._
import com.flaminem.flamy.utils.logging.Logging
import com.flaminem.flamy.utils.time.TimeUtils
import com.typesafe.config._
import org.apache.commons.configuration.{Configuration, PropertiesConfiguration}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConversions._


object FlamyContext extends Logging {

  private def getNewProcessId: String = {
    TimeUtils.nowToFileTime + "_" + ManagementFactory.getRuntimeMXBean.getName
  }

  var processId: String = getNewProcessId

  def reset(): Unit = {
    processId = getNewProcessId
  }

  def getPropertyURL(path: Option[String] = None): URL = {
    val url: URL =
      path match {
        case None => this.getClass.getClassLoader.getResource(FlamyConfVars.CONF_PATH)
        case Some(p) => new File(p).toURI.toURL
      }
    //noinspection ScalaStyle
    if(url==null) {
      throw new ConfigurationException(s"Could not find the configuration file (${FlamyConfVars.CONF_PATH}) in classpath.")
    }
    url
  }

  def loadConf(url: URL, conf: Map[String, String]): Config = {
    logger.info("Properties file used : " + url)
    System.setProperty("config.url", url.toString)
    ConfigFactory.invalidateCaches()
    val baseConf = ConfigFactory.load(ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF))
    /*
     * For some reason, TypeSafe really doesn't want to help the user to perform substitution
     * on programmatically added values. Therefore, we have to do this ugly workaround.
     */
    ConfigFactory.parseString(conf.map{case (k, v) => s"$k=$v"}.mkString("\n")).withFallback(baseConf).resolve()
  }

  def getPropertyURL(propertyFilePath: String): URL = {
    try {
      new File(propertyFilePath).toURI.toURL
    }
    catch {
      case e: MalformedURLException =>
        throw new ConfigurationException("Malformed URL : " + propertyFilePath)
    }
  }
}


class FlamyContext private (
  val globalOptions: FlamyGlobalOptions,
  override val env: Environment,
  val propertiesFileUrl: URL
)
extends FlamyConfVars(env, FlamyContext.loadConf(propertiesFileUrl, globalOptions.conf)) with Logging {

  def this(globalOptions: FlamyGlobalOptions, env: Option[Environment]) = {
    this(globalOptions, env.getOrElse(FlamyConfVars.MODEL_ENV), FlamyContext.getPropertyURL(globalOptions.configFile))
  }

  def this(project: String, environment: Environment) = {
    this(new FlamyGlobalOptions(conf = Map("flamy.project" -> project)), environment, FlamyContext.getPropertyURL())
  }

  def this(globalOptions: FlamyGlobalOptions) = {
    this(globalOptions, FlamyConfVars.MODEL_ENV, FlamyContext.getPropertyURL(globalOptions.configFile))
  }

  def this(properties: (String, String)*) = {
    this(new FlamyGlobalOptions(conf = properties.toMap))
  }

  def this(project: String) = {
    this("flamy.project" -> project)
  }

  private def init() = {
    FlamyGlobalContext.init(conf)
    RunDirCleaner.cleanRunDir(this)
    FlamyOutput.setLogLevel(FlamyGlobalContext.VERBOSITY_LEVEL.getProperty)
    logConf()
  }

  init()

  val modelDirs: List[File] = {
    MODEL_DIR_PATHS.getProperty.map{new File(_)}
  }

  private val variables: Variables = {
    val variablesPath: Option[String] = VARIABLES_PATH.getProperty
    val vars =
      if (variablesPath.isDefined) {
        MetaParser.parseVariables(variablesPath.get)
      }
      else {
        new Variables
      }
    vars ++= globalOptions.variables
    vars
  }

  /* TODO: This should probably not be a var */
  var dryRun: Boolean = false

  private lazy val localFS = {
    new SimpleFileSystem(FileSystem.get(new URI("file:///"), new org.apache.hadoop.conf.Configuration()))
  }

  def getLocalFileSystem: SimpleFileSystem = localFS

  private lazy val functionFetcher = {
    HiveFunctionFetcher(this)
  }

  def getFunctionFetcher: ModelHiveFunctionFetcher = {
    functionFetcher
  }

  private lazy val fileIndex: FileIndex = FileIndex(this)

  def getFileIndex: FileIndex = fileIndex

  def getVariables: Variables = variables

  def getEnvironment: Environment = env

  def getProject: String = PROJECT.getProperty.getOrElse("")

  def getHivePresetsPath: Option[String] = HIVE_PRESETS_PATH.getProperty

  def getHadoopBinPath: Option[String] = {
    Option(System.getenv("HADOOP_HOME")).map{_ + "/bin/hadoop"}
  }

  def getUdfJarPaths: Array[String] = {
    UDF_CLASSPATH.getProperty.map{_.split(":")}.getOrElse(Array())
  }

  private lazy val udfClassLoader: URLClassLoader = {
    val urls = getUdfJarPaths.map{new File(_).toURI.toURL}
    new URLClassLoader(urls, this.getClass.getClassLoader)
  }

  def getUdfClassLoader: URLClassLoader = {
    udfClassLoader
  }

  override def toString: String = {
    "FlamyContext [metaDirs=" + modelDirs +
      ", variables=" + variables +
      ", propertiesFileURL=" + propertiesFileUrl +
      ", env=" + env + "]"
  }

}

