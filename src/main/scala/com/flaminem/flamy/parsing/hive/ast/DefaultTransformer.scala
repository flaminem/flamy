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

import com.flaminem.flamy.parsing.hive.HiveParserUtils._
import org.apache.hadoop.hive.ql.parse.ASTNode

/**
  * Recursively applies a set of rules to the children of the node
  */
class DefaultTransformer[Context](ruleSet: => RuleSet[Context, Seq[ASTNode]]) extends Rule[Context, Seq[ASTNode]] {
  def apply(pt: ASTNode, context: Context): Seq[ASTNode] = {
    pt.transformChildren(ruleSet(_, context))::Nil
  }
}
