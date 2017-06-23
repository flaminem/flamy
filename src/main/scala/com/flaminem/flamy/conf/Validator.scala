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

object Validator {

  /**
    * The configuration value must be specified in the conf or have a default value.
    * An Exception will be thrown when fetching this value if it is not specified and has no default value.
    * @tparam T
    * @return
    */
  def required[T]: Validator[T] = new Validator[T] {

    /**
      * The name of this validator is "default" if a default value exists, "required" otherwise.
      * @param hasDefaultValue this indicates if the ConfVar has a default value or not.
      * @return
      */
    /* required and optional have the same behavior if a default value is defined, this is why they have the same name in such case.
     * The other option would have to have 3 validators: required, optional and default, but there was no obvious way
     * to make sure that there is no inconsistency (like a ConfVar being required with an optional value).
     */
    override def name(hasDefaultValue: Boolean) = {
      if(hasDefaultValue){
        "default"
      }
      else {
        "required"
      }
    }
    override def apply(value: Option[T]): Boolean = value.isDefined
    override def errorMessage(varName: String, value: Option[T]): String = s"$varName is required"
  }

  /**
    * The configuration value is optional, and it must be an Option.
    * If this value is not specified and has no default value, the program should be able to continue.
    * @tparam T
    * @return
    */
  def optional[T]: Validator[Option[T]] = new Validator[Option[T]] {

    /**
      * The name of this validator is "default" if a default value exists, "optional" otherwise.
      * @param hasDefaultValue this indicates if the ConfVar has a default value or not.
      * @return
      */
    override def name(hasDefaultValue: Boolean): String  = {
      if(hasDefaultValue){
        "default"
      }
      else {
        "optional"
      }
    }
    override def apply(value: Option[Option[T]]): Boolean = true
    override def errorMessage(varName: String, value: Option[Option[T]]): String = s"$varName is optional"
  }

  /**
    * The configuration value should be one of the specified <code>values<code>
    * @param values
    * @tparam T
    * @return
    */
  def in[T](values: T*): Validator[T] = new Validator[T] {
    override def name(hasDefaultValue: Boolean): String  = {
      s"in [${values.mkString(", ")}]"
    }
    override def apply(value: Option[T]): Boolean = {
      value match {
        case None => false
        case Some(v) => values.contains(v)
      }
    }
    override def errorMessage(varName: String, value: Option[T]): String = {
      value match {
        case None => s"$varName is not defined and has no default value."
        case Some(v) => s"$varName is equal to $v but should belong to ${values.mkString("[",",","]")}"
      }

    }
  }

}

/**
  * Validators are used to check if a configuration parameter value is correct.
  * The validation is lazy, which means that a configuration value will only be validated when it is fetched.
  */
trait Validator[T] {

  /**
    * This method will be used to check the configuration value (or default value if no value is found).
    * It must return true if the value is correct, false otherwise.
    * @param value
    * @return
    */
  def apply(value: Option[T]): Boolean

  /**
    * The error message to display if the configuration value is not valid.
    * @param varName
    * @param value
    * @return
    */
  def errorMessage(varName: String, value: Option[T]): String

  /**
    * The name of the validator. It may depend on the fact that the ConfVar has a default value or not.
    * @param hasDefaultValue this indicates if the ConfVar has a default value or not.
    * @return
    */
  def name(hasDefaultValue: Boolean): String
}