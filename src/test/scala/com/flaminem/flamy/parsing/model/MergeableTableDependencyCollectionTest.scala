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

import com.flaminem.flamy.model.TableType
import org.scalatest.{FreeSpec, Matchers}

/**
  * Created by fpin on 4/16/16.
  */
class MergeableTableDependencyCollectionTest extends FreeSpec with Matchers {


  "MergeableTableDependencyCollection should be correct" in {
    val td = new TableDependency(TableType.TEMP,"db.test")
    val set = new MergeableTableDependencyCollection(Seq(td))
    assert(set.get("db.test") === Some(td))


  }




}
