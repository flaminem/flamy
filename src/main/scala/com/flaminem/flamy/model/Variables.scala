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

package com.flaminem.flamy.model

import com.flaminem.flamy.conf.{FlamyContext, FlamyGlobalContext}
import com.flaminem.flamy.model.exceptions.{FlamyException, UndefinedVariableException}
import com.flaminem.flamy.parsing.hive.QueryUtils
import org.rogach.scallop.ValueConverter

import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.util.matching.Regex

// TODO: Variables should be immutable
class Variables extends mutable.HashMap[String,String] {

  import Variables._

  def this(vars: (String, String)*) {
    this()
    this ++= vars
  }


  /**
    * Replace partitions variables with dummy values, in order to make the Semantic and Type analysis pass.
    * For instance, the value "${partition:p1}0" is replaced with 0
    * and the value "${partition:p2}'0'" is replaced with '0'
    * @param partitions
    * @return
    */
  def cancelPartitions(partitions: Seq[PartitionKey]): Variables = {
    val res: Seq[(String, String)] =
      this.toSeq.map{
        case (name, partitionVariableRE(_, value)) => name -> value
        case (name, value) => name -> value
      }
    new Variables(res:_*)
  }

  /**
    * If quotePartitions is true, add quotes around values of the form ${partition:varName}.
    * For instance, the value "${partition:p1}0" is replaced with "${partition:p1}"
    * and the value "${partition:p2}'0'" is replaced with "${partition:p2}"
    * @param varValue
    * @return
    */
  private def correctPartitionValue(varValue: String): String = {
    varValue match {
      case partitionVariableRE(name, _) => "\"" + name + "\""
      case _ => varValue
    }
  }

  /**
    * Replaces all occurrences of variables in given text by their values.
    * If quotePartitions is true, the partition variables values are put into quotes (useful for parsing) .
    *
    * @param text
    * @return
    * @throws UndefinedVariableException
    */
  @throws (classOf[UndefinedVariableException])
  def replaceInText(text: String): String = {
    var result = text
    val undefinedVariables: mutable.TreeSet[String] = new mutable.TreeSet[String]
    val alreadyReplaced = mutable.Set[String]()
    for (m <- variableRE.findAllMatchIn(text)) {
      val varName: String = m.group(1)
      if (this.contains(varName)) {
        if(!alreadyReplaced.contains(varName)){
          alreadyReplaced += varName
          val varValue: String = correctPartitionValue(this(varName))
          result = result.replaceAllLiterally(s"$${$varName}", varValue)
        }
      }
      else {
        undefinedVariables += "${" + varName + "}"
      }
    }
    if (undefinedVariables.nonEmpty) {
      throw new UndefinedVariableException(undefinedVariables)
    }
    result
  }

  /**
    * Returns the subset of variables present in the given text.
    * a sequence of PartitionColumn may be passed as second argument:
    * when a partition variable (${partition:[name]}) is found in the list, we return its associated value
    * if it has one, or we return 0 or "0" depending on the type of the partition.
    * If no value nor type is found, we throw an error.
    * @param text
    * @param partitions
    * @return
    */
  def subsetInText(text: String, partitions: Iterable[PartitionColumn] = Nil): Variables = {
    var result: Variables = new Variables
    for (m <- variableRE.findAllMatchIn(QueryUtils.cleanQuery(text))) {
      val varName: String = m.group(1)
      if (this.contains(varName)) {
        val varValue: String = this(varName)
        result += (varName -> varValue)
      }
      else {
        result += (varName -> getPartitionValue(varName, partitions))
      }
    }
    result
  }

  override def toString(): String = {
    this.mkString("{",", ","}")
  }
}

object Variables {

  private val variableRE: Regex = "[$]\\{([^\\}]*)\\}".r
  private val partitionVariableRE: Regex = """\A([$][{]partition:.*[}])(.*)\z""".r


  def apply(vars: (String, String)*): Variables = {
    var res = new Variables()
    vars.foreach{res+=_}
    res
  }

  /**
    * Create a set of variables from a sequence of arguments
    * @param l
    * @return
    */
  private def fromArgs(l: Seq[String]): Either[String, Option[Variables]] = {
    val res = new Variables
    if(l.exists{!_.contains("=")}){
      Left("")
    }
    else{
      for {
        string <- l
        if string.contains("=")
      } {
        val Array(k, v) = string.split("=", 2)
        res += k -> v
      }
      Right(Some(res))
    }
  }

  implicit val scallopConverter: ValueConverter[Variables] = {
    new ValueConverter[Variables] {
      override def parse(s: List[(String, List[String])]): Either[String, Option[Variables]] = {
        s match {
          case l if l.nonEmpty => fromArgs(l.flatMap{_._2})
          case Nil => Right(None)
        }
      }

      override val tag: TypeTag[Variables] = typeTag[Variables]
      override val argType = org.rogach.scallop.ArgType.LIST
    }
  }

  /**
    * Returns the subset of variables names present in the given text.
    * @param text
    * @return
    */
  def findInText(text: String): Seq[String] = {
    for {
      m <- variableRE.findAllMatchIn(QueryUtils.cleanQuery(text)).toSeq
    } yield {
        val varName: String = m.group(1)
        varName
    }
  }

  /**
    * Partition Variables values are defined this way:
    * ${partition:p1} where p1 is an int is replaced with "${partition:p1}0"
    * ${partition:p1} where p2 is a string is replaced with "${partition:p2}'0'"
    */
  private def getPartitionValue(varName: String, partitions: Iterable[PartitionColumn]): String = {
    if (varName.startsWith("partition:")) {
      val partName = varName.split(":", 2)(1).toLowerCase()
      partitions.find {_.columnName == partName} match {
        case None =>
          val message =
            if(FlamyGlobalContext.USE_OLD_REGEN.getProperty) {
              s"""The following partition variables are not defined : $varName.
                |Only partition names from the source table are allowed.
                |Known partition names are: ${partitions.map{_.columnName}.mkString(", ")}""".stripMargin
            }
            else {
              s"""The following partition variables are not defined : $varName.
                 |Only partition names from the destination table are allowed.
                 |Known partition names are: ${partitions.map{_.columnName}.mkString(", ")}""".stripMargin
            }
          throw new FlamyException(message)
        case Some(p) if p.value.isDefined => p.stringValue
        case Some(p) =>
          p.columnType.map{_.toLowerCase} match {
            case Some("tinyint") | Some("smallint") | Some("int") | Some("bigint") => "${" + varName + "}0"
            case _ => "${" + varName + "}'0'"
          }
      }
    }
    else {
      throw new UndefinedVariableException(varName::Nil)
    }
  }

  /**
    * Put quotes around every variable in text. Used for unit tests.
    *
    * @param text
    * @return
    * @throws UndefinedVariableException
    */
  @throws (classOf[UndefinedVariableException])
  def quoteAllVariablesInText(text: String): String = {
    var result = text
    val alreadyReplaced = mutable.Set[String]()
    for (m <- variableRE.findAllMatchIn(text)) {
      val varName: String = m.group(1)
      if (!alreadyReplaced.contains(varName)) {
        alreadyReplaced += varName
        val varValue: String = "\"" + varName + "\""
        result = result.replaceAllLiterally(s"$${$varName}", varValue)
      }
    }
    result
  }

}