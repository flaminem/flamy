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

package com.flaminem.flamy.parsing.hive.ast

import com.flaminem.flamy.model.exceptions.{FlamyException, UnexpectedBehaviorException}
import com.flaminem.flamy.parsing.hive.HiveParserUtils.drawTree
import org.apache.hadoop.hive.ql.parse.ASTNode

import scala.collection.immutable.HashMap
import scala.util.control.NonFatal


trait Rule[-Context, +Out] {

  def apply(pt: ASTNode, context: Context): Out

}

/**
  * A rule that only applies to one given type of nodes.
  * @param tokenType
  * @tparam Context
  * @tparam Out
  */
abstract class SingleRule[-Context, +Out](
  /**
    * The HiveParser token type that this transformer matches
    */
  val tokenType: Int
) extends Rule[Context, Out] {

  def apply(pt: ASTNode, context: Context): Out

}

/**
  * A set of SingleRules, with a default rule to apply when no single rule matches.
  * @param defaultRule
  * @param map
  * @tparam Context
  * @tparam Out
  */
class RuleSet[Context, Out](
  val defaultRule: Rule[Context, Out],
  val map: Map[Int, Rule[Context, Out]] = HashMap()
) extends Rule[Context, Out] {

  def this(defaultRule: Rule[Context, Out], singleTransformers: SingleRule[Context, Out]*) {
    this(defaultRule, singleTransformers.map{t => (t.tokenType, t)}.toMap)
  }

  def apply(pt: ASTNode, context: Context): Out = {
    try{
      map.getOrElse(pt.getType, defaultRule).apply(pt, context)
    }
    catch{
      case e: FlamyException if !e.isInstanceOf[UnexpectedBehaviorException] => throw e
      case NonFatal(e) =>
        throw new UnexpectedBehaviorException(s"Tree is:\n${drawTree(pt)}", e)
    }
  }
}

