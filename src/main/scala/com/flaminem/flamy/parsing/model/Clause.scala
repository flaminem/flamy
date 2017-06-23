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

import com.flaminem.flamy.model.columns.ColumnValue

/**
  * A Boolean clause, that can be used in WHERE, HAVING, etc.
  */
trait Clause {

  def transformChildren(f: (Clause) => Clause): Clause

  /**
    * Apply a transformation function to every level of the tree, starting by the leaves.
    * @param pf
    * @return
    */
  def transformUp(pf: PartialFunction[Clause, Clause]): Clause = {
    pf.applyOrElse(transformChildren{_.transformUp(pf)}, identity[Clause])
  }

  /**
    * Apply a transformation function to every level of the tree, starting by the root.
    * @param pf
    * @return
    */
  def transformDown(pf: PartialFunction[Clause, Clause]): Clause = {
    pf.applyOrElse(this, identity[Clause]).transformChildren{_.transformDown(pf)}
  }

}


object Clause {

  type Expr = ColumnValue

  /**
    * This clause is represent a clause that may be true.
    * When doing partition pruning, we want to filter out clause that we know are false.
    */
  case object Maybe extends Clause {
    override def transformChildren(f: (Clause) => Clause): Clause = this
  }

  case object True extends Clause {
    override def transformChildren(f: (Clause) => Clause): Clause = this
  }

  case object False extends Clause {
    override def transformChildren(f: (Clause) => Clause): Clause = this
  }

  case class ColumnClause(columnValue: ColumnValue) extends Clause {
    override def transformChildren(f: (Clause) => Clause): Clause = this
  }



  case class And(left: Clause, right: Clause) extends Clause {
    override def transformChildren(f: (Clause) => Clause): Clause = {
      And(f(left), f(right))
    }
  }
  /** Allows to write "clause1 And clause2" */
  implicit class ClauseAnd(clause: Clause) {
    def And(that: Clause): Clause = new And(clause, that)
  }



  case class Or(left: Clause, right: Clause) extends Clause {
    override def transformChildren(f: (Clause) => Clause): Clause = {
      Or(f(left), f(right))
    }
  }
  /** Allows to write "clause1 Or clause2" */
  implicit class ClauseOr(clause: Clause) {
    def Or(that: Clause): Clause = new Or(clause, that)
  }



  case class Not(clause: Clause) extends Clause {
    override def transformChildren(f: (Clause) => Clause): Clause = {
      Not(f(clause))
    }
  }

  case class Like(left: Expr, right: Expr) extends Clause {
    override def transformChildren(f: (Clause) => Clause): Clause = this



  }

  case class RLike(left: Expr, right: Expr) extends Clause {
    override def transformChildren(f: (Clause) => Clause): Clause = this


  }

  class Comparison(val left: Expr, val right: Expr) extends Clause {
    override def transformChildren(f: (Clause) => Clause): Clause = this



  }

  case class Equal(override val left: Expr, override val right: Expr) extends Comparison(left, right){



  }

  /* Correspond to the symbol <=> : nulls are considered as equal */
  case class EqualNS(override val left: Expr, override val right: Expr) extends Comparison(left, right){



  }

  case class NotEqual(override val left: Expr, override val right: Expr) extends Comparison(left, right){



  }

  case class LowerThan(override val left: Expr, override val right: Expr) extends Comparison(left, right){



  }

  case class LowerOrEqual(override val left: Expr, override val right: Expr) extends Comparison(left, right){



  }

  case class GreaterThan(override val left: Expr, override val right: Expr) extends Comparison(left, right){



  }

  case class GreaterOrEqual(override val left: Expr, override val right: Expr) extends Comparison(left, right){



  }

}