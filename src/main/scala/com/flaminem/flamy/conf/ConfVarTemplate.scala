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

import java.util

import com.flaminem.flamy.conf.ConfLevel.ConfLevel
import com.typesafe.config.Config
import org.apache.commons.configuration.Configuration

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

/**
  * A template of self-extracting configuration property that read its value from an Apache Configuration
  */
trait ConfVarTemplate[T] {

  def confLevel: ConfLevel
  def varName: String
  def defaultValue: Option[T]
  def validator: Validator[T]
  def description: String

  implicit def typeTag: TypeTag[T]

  /**
    * The name of the property key to lookup in the Configuration
    */
  def propertyKey: String

  def conf: Config

  private def asString: Option[String] = {
    if(conf.hasPath(propertyKey)){
      Option(conf.getString(propertyKey))
    }
    else {
      None
    }
  }

  private def asInt: Option[Int] = {
    if(conf.hasPath(propertyKey)){
      Option(conf.getInt(propertyKey))
    }
    else {
      None
    }
  }

  private def asBoolean: Option[Boolean] = {
    if(conf.hasPath(propertyKey)){
      Option(conf.getBoolean(propertyKey))
    }
    else {
      None
    }
  }

  private def asStringList: Option[List[String]] = {
    if(conf.hasPath(propertyKey)){
      conf.getAnyRef(propertyKey) match {
        case s : String => Some(s::Nil)
        case l : util.List[String] => Some(l.toList)
        case _ => throw new ConfigurationException(
          s"Configuration property $varName should be a list. " +
            """The syntax for list is "[elem1, elem2]. For a singleton, brackets may be ommitted."""".stripMargin
        )
      }
    }
    else {
      None
    }
  }

  private def readConf: Option[T] = {
    typeOf[T] match {
      case q if q == typeOf[String] => asString.map{_.asInstanceOf[T]}
      case q if q == typeOf[Option[String]] => Option(asString.asInstanceOf[T])
      case q if q == typeOf[Boolean] => asBoolean.map{_.asInstanceOf[T]}
      case q if q == typeOf[Option[Boolean]] => Option(asBoolean.asInstanceOf[T])
      case q if q == typeOf[Int] => asInt.map{_.asInstanceOf[T]}
      case q if q == typeOf[Option[Int]] => Option(asInt.asInstanceOf[T])
      case q if q == typeOf[List[String]] => asStringList.map{_.asInstanceOf[T]}
      case _ => throw new ConfigurationException(f"Configuration property $varName : type ${typeOf[T]} is not supported yet.")
    }
  }

  @throws[ConfigurationException]
  private var cachedValue: Option[T] = None

  final def resetCachedValue(): Unit = {
    cachedValue = None
  }

  private def getValue: T = {
    val value: Option[T] = readConf match {
      case None => defaultValue
      case v: Some[T] => v
      case _ => throw new ConfigurationException(f"Configuration property $varName is expected to be of type ${typeOf[T]}")
    }
    if(!validator(value)) {
      throw new ConfigurationException(validator.errorMessage(propertyKey,value))
    }
    typeOf[T] match {
      case q if value.isEmpty && q =:= typeOf[Option[_]] => None.asInstanceOf[T]
      case q if value.isEmpty => throw new ConfigurationException(f"Configuration property $varName is not defined and has no default value")
      case _ => value.get
    }
  }

  final def getProperty: T = {
    cachedValue.getOrElse{
      val value = getValue
      cachedValue = Some(value)
      value
    }
  }

  private[conf] def getStringFormattedProperty: String = {
    val value = getProperty
    typeOf[T] match {
      case q if q =:= typeOf[Option[_]] =>
        value.asInstanceOf[Option[_]]  match {
          case Some(v) => v.toString
          case None => "None"
        }
      case q if q == typeOf[List[String]] => value.asInstanceOf[List[String]].mkString("[",",","]")
      case _ => value.toString
    }
  }

}
