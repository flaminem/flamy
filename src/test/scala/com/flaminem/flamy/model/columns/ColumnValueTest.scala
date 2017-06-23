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

package com.flaminem.flamy.model.columns

import org.scalatest.{FreeSpec, Matchers}

/**
  * Created by fpin on 9/11/16.
  */
class ColumnValueTest extends FreeSpec with Matchers {


  "apply should work correctly" in {

    assert( ColumnValue(None) === NoValue )
    assert( ColumnValue("a") === ConstantValue("a") )
    assert( ColumnValue("${a}") === VariableValue("${a}") )

  }






}
