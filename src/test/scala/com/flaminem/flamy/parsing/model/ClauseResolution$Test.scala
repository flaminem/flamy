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

import com.flaminem.flamy.parsing.model.Clause._
import org.scalatest.{FreeSpec, Matchers}


/**
  * Created by fpin on 9/28/16.
  */
class ClauseResolution$Test extends FreeSpec with Matchers {


  "simple resolution rules should work" in {
    assert((True And True).transformUp(ClauseResolution.simpleRules) === True)
    assert((True Or False).transformUp(ClauseResolution.simpleRules) === True)
    assert((True And False).transformUp(ClauseResolution.simpleRules) === False)
    assert( ((True And True) Or (False And False)).transformUp(ClauseResolution.simpleRules) === True)
    assert( ((True And Maybe) Or (False And Maybe)).transformUp(ClauseResolution.simpleRules) === Maybe)
    assert( (Not((True And Maybe) Or (False And False)) And ((True And Maybe) Or (False And Maybe))).transformUp(ClauseResolution.simpleRules) === Maybe)
  }


}
