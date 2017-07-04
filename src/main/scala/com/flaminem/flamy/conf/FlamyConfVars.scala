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

import com.flaminem.flamy.conf.ConfLevel._
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.utils.logging.Logging
import com.flaminem.flamy.utils.macros.SealedValues
import org.apache.commons.configuration.Configuration
import com.typesafe.config.Config

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

object FlamyConfVars {
  val MODEL_ENV: Environment = Environment("model")
  val CONF_PATH: String = "flamy.properties"
  val ENV_PREFIX : String = "env"
}

/**
  * Configuration properties that can have multiple instance in the same JVM,
  * and can either be global or depend on a project or an environment
  */
class FlamyConfVars(val env: Environment, val conf: Config) extends Logging { self =>
  import com.flaminem.flamy.conf.FlamyConfVars._

  checkProject()

  /**
    * When multi-project mode is not enabled, no project prefix is required
    */
  val projectString: String = PROJECT.getProperty match {
    case None => ""
    case Some(p) => "." + p
  }


  def checkProject(): Unit = {
    val projects: List[String] = PROJECTS.getProperty
    PROJECT.getProperty match {
      case Some(p) =>
        if (!projects.contains(p)) {
          throw new ConfigurationException(f"Project $p must be declared in '${Flamy.name}.${PROJECTS.varName}'")
        }
      case None =>
        if (projects.nonEmpty) {
          throw new ConfigurationException(
            f"Multi-project mode is enabled: please choose a project with '${Flamy.name} --project <PROJECT> command', " +
              f"or remove '${Flamy.name}.${PROJECTS.varName}' from your configuration file. Available projects are : ${projects.mkString(", ")}")
        }
    }
  }

  ///////////////////////
  // Global properties //
  ///////////////////////

  object PROJECTS extends
    ConfVar[List[String]](
      confLevel = Global,
      varName = "projects",
      defaultValue = Some(Nil),
      validator = Validator.required,
      description = s"Comma-separated list of projects. Only necessary if multiple projects are configured."
    )

  object PROJECT extends
    ConfVar[Option[String]](
      confLevel = Global,
      varName = "project",
      defaultValue = None,
      validator = Validator.optional,
      description = "Select the project you want to use here. Only necessary if multiple projects are configured."
    )

  object UDF_CLASSPATH extends
    ConfVar[Option[String]](
      confLevel = Global,
      varName = "udf.classpath",
      defaultValue = None,
      validator = Validator.optional,
      description = "list of jar paths (separated with ':') where flamy will look for the custom Hive UDFs."
    )

  object PARALLELISM extends
    ConfVar[Int](
      confLevel = Global,
      varName = "exec.parallelism",
      defaultValue = Some(5),
      validator = Validator.required,
      description = "Controls the maximum number of jobs that flamy is allowed to run simultaneously."
    )

  object VERBOSITY_LEVEL extends
    ConfVar[String](
      confLevel = Global,
      varName = "verbosity.level",
      defaultValue = Some("INFO"),
      validator = Validator.in(FlamyOutput.logLevelNames:_*),
      description = "Controls the verbosity level of flamy."
    )

  ////////////////////////
  // Project properties //
  ////////////////////////

  object MODEL_DIR_PATHS extends ConfVar[List[String]](
    confLevel = Project,
    varName = "model.dir.paths",
    defaultValue = None,
    validator = Validator.required,
    description = "Space-separated list of folder paths where flamy will look for the SQL files of your model."
  )

  object VARIABLES_PATH extends ConfVar[Option[String]](
    confLevel = Project,
    varName = "variables.path",
    defaultValue = None,
    validator = Validator.optional,
    description = "TODO"
  )

  ////////////////////////////
  // Environment properties //
  ////////////////////////////

  object HDFS_CONF_DIR extends
    ConfVar[String](
      confLevel = Env,
      varName = "hdfs.conf.dir",
      defaultValue = None,
      validator = Validator.required,
      description = "The directory containing the configuration files for hadoop, like core-site.xml and hdfs-site.xml. " +
      "If the specified path is relative, it will be expanded from the directory containing the flamy.properties file."
    )

  object HIVE_PRESETS_PATH extends
    ConfVar[Option[String]](
      confLevel = Env,
      varName = "hive.presets.path",
      defaultValue = None,
      validator = Validator.optional,
      description = "TODO"
    )

  object HIVE_META_FETCHER_TYPE extends
    ConfVar[String](
      confLevel = Env,
      varName = "hive.meta.fetcher.type",
      defaultValue = Some("default"),
      validator = Validator.in("direct", "client", "default"),
      description = "TODO"
    )

  object HIVE_METASTORE_JDBC_URI extends
    ConfVar[String](
      confLevel = Env,
      varName = "hive.metastore.jdbc.uri",
      defaultValue = None,
      validator = Validator.required,
      description = "TODO"
    )

  object HIVE_METASTORE_JDBC_USER extends
    ConfVar[String](
      confLevel = Env,
      varName = "hive.metastore.jdbc.user",
      defaultValue = Some("flamy"),
      validator = Validator.required,
      description = "TODO"
    )

  object HIVE_METASTORE_JDBC_PASSWORD extends
    ConfVar[String](
      confLevel = Env,
      varName = "hive.metastore.jdbc.password",
      defaultValue = Some("flamyPassword"),
      validator = Validator.required,
      description = "TODO"
    )

  object HIVE_METASTORE_URI extends
    ConfVar[String](
      confLevel = Env,
      varName = "hive.metastore.uri",
      defaultValue = None,
      validator = Validator.required,
      description = "Thrift URI of the Hive Metastore. Used in client mode of the meta.fetcher"
    )

  object HIVE_SERVER_URI extends
    ConfVar[String](
      confLevel = Env,
      varName = "hive.server.uri",
      defaultValue = None,
      validator = Validator.required,
      description = "URI of the Hive Server 2"
    )

  object HIVE_SERVER_LOGIN extends
    ConfVar[String](
      confLevel = Env,
      varName = "hive.server.login",
      defaultValue = Some(SystemContext.userName),
      validator = Validator.required,
      description = "Login used to connect to the Hive Server 2"
    )

  protected def logConf(): Unit = {
    conf.root().entrySet().foreach{
      case entry => logger.info(entry.getKey + " = " + entry.getValue)
    }
  }

  def getPossibleEnvironments: List[Environment] = {
    if(conf.hasPath(s"${Flamy.name}$projectString.$ENV_PREFIX")){
      conf.getConfig(s"${Flamy.name}$projectString.$ENV_PREFIX")
        .root().entrySet().toList
        .map{_.getKey}.map{Environment(_)}
    }
    else {
      Nil
    }
  }

  sealed class ConfVar[T: TypeTag] (
    override val confLevel: ConfLevel,
    override val varName: String,
    override val defaultValue: Option[T],
    override val validator: Validator[T],
    override val description: String,
    override val hidden: Boolean = false
  )(implicit override val typeTag: TypeTag[T]) extends ConfVarTemplate[T] {

    override def conf: Config = self.conf

    def propertyKey: String = confLevel match {
      case Global  => s"${Flamy.name}.$varName"
      case Project => s"${Flamy.name}$projectString.$varName"
      case Env     => s"${Flamy.name}$projectString.$ENV_PREFIX.$env.$varName"
    }

  }

  /* This line must stay after the value declaration or it will be empty */
  val confVars: Seq[ConfVar[_]] = SealedValues.values[ConfVar[_]]

}
