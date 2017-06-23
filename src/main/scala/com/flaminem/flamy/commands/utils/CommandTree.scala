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

package com.flaminem.flamy.commands.utils

/**
  * Created by fpin on 2/16/17.
  */
case class CommandTree(name: String, children: Seq[CommandTree]) {

  def drill(commands: Seq[String]): Seq[CommandTree] = {
    commands match {
      case Nil => this.children
      case head::tail =>
        children.find{_.name == head} match {
          case None => Nil
          case Some(c) => c.drill(tail)
        }
    }
  }

  def jsonify: String = {
    val sub = children.map{_.jsonify}.mkString("{",",","}")
    if (name == "") {
      sub
    }
    else{
      s""""$name":$sub"""
    }
  }

}
