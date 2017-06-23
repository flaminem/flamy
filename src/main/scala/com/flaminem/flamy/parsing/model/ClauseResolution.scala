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

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
  * Created by fpin on 9/28/16.
  */
object ClauseResolution {

  class Rule[+T <: Clause: ClassTag](pf: PartialFunction[T, Clause]) extends PartialFunction[Clause, Clause] {

    override def isDefinedAt(x: Clause): Boolean = {
      x match {
          /* The ClassTag is required to match on the generic type T */
        case y : T => pf.isDefinedAt(y)
        case _ => false
      }
    }

    override def apply(x: Clause): Clause = {
      pf.apply(x.asInstanceOf[T])
    }

  }

  object Rule {

    def apply[T <: Clause: ClassTag](pf: PartialFunction[T, Clause]): Rule[T] = new Rule(pf)

  }

//  type Rule = PartialFunction[Clause, Clause]

  import com.flaminem.flamy.parsing.model.Clause._

  val simpleRules: Seq[Rule[Clause]] = Seq(
    Rule[And]{
      case _ And False => False
      case False And _ => False
      case Maybe And _ => Maybe
      case _ And Maybe => Maybe
      case True And True => True
    },
    Rule[Or]{
      case _ Or True => True
      case True Or _ => True
      case Maybe Or _ => Maybe
      case _ Or Maybe => Maybe
    },
    Rule[Not]{
      case Not(False) => True
      case Not(True) => False
      case Not(Maybe) => Maybe
    }
  )

  implicit def rulesApplier(rules: Seq[Rule[Clause]]): PartialFunction[Clause, Clause] = {
    case clause: Clause =>
      rules.foldLeft(clause) {
        case (c, rule) if rule.isDefinedAt(c) => rule.apply(c)
        case (c, _) => c
      }
  }

}



