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

package com.flaminem.flamy.utils

/**
  * Create a resource that will be automatically closed after being used.
  * The proper syntax to use it, is with a for ... syntax.
  * @example {{{
  *   val result: Result =
  *   for {
  *     con <- Resource(new Connection(...))
  *     statement <- Resource(new Statement(con))
  *   } yield {
  *     statement.exec(...): Result
  *   }
  *   // con and statement are now closed
  * }}}
  * Unfortunately, this is not compatible with intermediate value definition
  */
class AutoClose[A](val resource: A, val closingFunction: (A) => Unit) {

  def close(): Unit = {
    closingFunction(resource)
  }

  def foreach[U](f: (A) => U): Unit = {
    try {
      f(resource)
    }
    finally {
      close()
    }
  }

  def map[B](f: (A) => B): B = {
    try {
      f(resource)
    }
    finally {
      close()
    }
  }

  def flatMap[B](f: (A) => B): B = {
    try {
      f(resource)
    }
    finally {
      close()
    }
  }

  def filter(f: (A) => Boolean): AutoClose[A] = {
    this
  }

  def withFilter(f: (A) => Boolean): AutoClose[A] = {
    this
  }

}

/**
  * Create a resource that will be automatically closed after being used.
  * The proper syntax to use it, is with a for ... syntax.
  * @example {{{
  *   val result: Result =
  *   for {
  *     con <- Resource(new Connection(...))
  *     statement <- Resource(new Statement(con))
  *   } yield {
  *     statement.exec(...): Result
  *   }
  *   // con and statement are now closed
  * }}}
  * Unfortunately, this is not compatible with intermediate value definition
  */
object AutoClose {

  /**
    * Create a resource that will be automatically closed after being used.
    * The proper syntax to use it, is with a for ... syntax.
    * @example {{{
    *   val result: Result =
    *   for {
    *     con <- Resource(new Connection(...))
    *     statement <- Resource(new Statement(con))
    *   } yield {
    *     statement.exec(...): Result
    *   }
    *   // con and statement are now closed
    * }}}
    * Unfortunately, this is not compatible with intermediate value definition
    * @param resource
    * @tparam A
    * @return
    */
  def apply[A <: java.lang.AutoCloseable](resource: A): AutoClose[A] = {
    new AutoClose(resource, _.close())
  }

  /**
    * Create a resource that will be automatically closed after being used.
    * The proper syntax to use it, is with a for ... syntax.
    * @example {{{
    *   val result: Result =
    *   for {
    *     con <- Resource(new Connection(...))
    *     statement <- Resource(new Statement(con))
    *   } yield {
    *     statement.exec(...): Result
    *   }
    *   // con and statement are now closed
    * }}}
    * Unfortunately, this is not compatible with intermediate value definition
    * (cannot use "v = ..." inside the for clause)
    * @param resource
    * @param close
    * @tparam A
    * @return
    */
  def apply[A](resource: A, close: (A) => Unit): AutoClose[A] = {
    new AutoClose(resource, close)
  }

}