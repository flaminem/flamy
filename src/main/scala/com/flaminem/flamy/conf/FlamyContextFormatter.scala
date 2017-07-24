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

import com.flaminem.flamy.utils.prettyprint.Tabulator

import scala.language.existentials
import scala.util.{Failure, Success, Try}

/**
 * Created by fpin on 4/22/15.
 */
class FlamyContextFormatter(context: FlamyContext) {

  /**
    * Formats the used configuration as a a table
    * @return
    */
  def format(): String = {
    val confVars: Seq[ConfVarTemplate[_]] = context.confVars ++ FlamyGlobalContext.confVars
    val header: Seq[Any] =
      Seq(
         "level"
        ,"property"
        ,"value"
        ,"validator"
//        ,"default"
//        ,"description"
      )
    val table: Seq[Seq[Any]] =
      confVars.flatMap{
        confVar =>
          Try(confVar.getStringFormattedProperty) match {
            case Success(v) =>
              (
                (confVar.confLevel, confVar.propertyKey),
                Seq(
                  confVar.confLevel
                  ,confVar.propertyKey
                  ,v
                  ,confVar.validator.name(confVar.defaultValue.isDefined)
                  //                ,confVar.defaultValue
                  //                ,confVar.validator.name
                  //                ,confVar.description
                )
              ) :: Nil
            case Failure(e) =>
              Nil
          }
      }.toSeq.sortBy{_._1._2}.sortBy{_._1._1}.map{_._2}
    new Tabulator(leftJustify = true).format(header+:table)
  }

  private def staticVars: Seq[FlamyGlobalContext.GlobalConfVar[_]] = FlamyGlobalContext.confVars.filterNot{_.hidden}
  private def globalVars: Seq[ConfVarTemplate[_]] = context.confVars.filter{_.confLevel == ConfLevel.Global}.filterNot{_.hidden}
  private def projectVars: Seq[context.ConfVar[_]] = context.confVars.filter{_.confLevel == ConfLevel.Project}.filterNot{_.hidden}
  private def envVars: Seq[context.ConfVar[_]] = context.confVars.filter{_.confLevel == ConfLevel.Env}.filterNot{_.hidden}
  private def confVars: Seq[ConfVarTemplate[_]] = (globalVars ++ projectVars ++ envVars ++ staticVars)

  /**
    * Formats the used configuration as a .properties template
    * @return
    */
  def toTemplate: String = {
      confVars.map{
        cv =>
          val default =
            if(cv.defaultValue.isDefined) {
             s"\n# default: ${cv.defaultValue.get}"
            }
            else {
             ""
            }
          s"""# ${cv.description}$default
             |${cv.propertyKey} =
             |
             |""".stripMargin
      }.mkString("")
  }


  private def confVarToMarkdown(cv: ConfVarTemplate[_]) = {
    val default =
      cv.defaultValue match {
        case Some(s: String) =>
          s"""(default: "${cv.defaultValue.get}")"""
        case Some(v) =>
          s"(default: ${cv.defaultValue.get})"
        case None => ""
      }
    val tpe =
      cv.validator match {
        case Validator.In(s) => s.map {"\"" + _.toString + "\""}.mkString(" \\| ")
        case _ => cv.typeTag.tpe.toString.replace("[","\\[")
      }
    s"`${cv.propertyKey}` $tpe  $default  \n*${cv.description}*\n"
  }

  /**
    * Formats the used configuration as a .markdown doc
    * @return
    */
  def toMarkdown: String = {
    "### Global properties\n" +
    (projectVars++globalVars).map{confVarToMarkdown}.mkString("\n") +
    "### Environment properties\n " +
    "These properties can be set for each environment you want to configure. Just replace `<ENV>` by the name of the correct environment\n\n" +
    envVars.map{confVarToMarkdown}.mkString("\n") +
    "### Other properties\n " +
    "These are additional, less used, properties.\n" +
    staticVars.map{confVarToMarkdown}.mkString("\n")
  }

  private def confVarToRST(cv: ConfVarTemplate[_]) = {
    val default =
      cv.defaultValue match {
        case Some(s: String) =>
          s"""(default: "${cv.defaultValue.get}")"""
        case Some(v) =>
          s"(default: ${cv.defaultValue.get})"
        case None => ""
      }
    val tpe =
      cv.validator match {
        case Validator.In(s) => s.map {"\"" + _.toString + "\""}.mkString(" \\| ")
        case _ => cv.typeTag.tpe.toString.replace("[","\\[")
      }
    s"``${cv.propertyKey}`` $tpe  $default  \n|br| *${cv.description}*\n"
  }

  /**
    * Formats the used configuration as a .markdown doc
    * @return
    */
  def toRST: String = {
    """.. |br| raw:: html
      |
      |   <br />
      |""".stripMargin +
    "\nGlobal properties\n\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\n" +
    (projectVars++globalVars).map{confVarToRST}.mkString("\n") +
    "\nEnvironment properties\n\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\n" +
    "These properties can be set for each environment you want to configure. Just replace ``<ENV>`` by the name of the correct environment\n\n" +
    envVars.map{confVarToRST}.mkString("\n") +
    "\nOther properties\n\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\n" +
    "These are additional, less used, properties.\n\n" +
    staticVars.map{confVarToRST}.mkString("\n")
  }



}
