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

package com.flaminem.flamy.parsing.model

/**
  * Represents a Node in the dependency tree of a query.
  * For instance, it may be a Union, a Join, etc.
  */
trait DependencyNode {



}


case object NoDeps extends DependencyNode


case class Join (
  left: DependencyNode,
  right: DependencyNode
) extends DependencyNode {


}


case class LeftSemiJoin (
  left: DependencyNode,
  right: DependencyNode
) extends DependencyNode {


}


case class Union (
  left: DependencyNode,
  right: DependencyNode
) extends DependencyNode {


}


object Union {

  def apply(deps: DependencyNode*): DependencyNode = {
    deps.toList match {
      case Nil => NoDeps
      case h::Nil => h
      case h::t => new Union(h, Union(t:_*))
    }

  }


}