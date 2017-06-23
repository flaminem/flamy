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

package com.flaminem.flamy.model.partitions

import com.flaminem.flamy.model.PartitionColumn
import org.scalatest.FreeSpec

/**
  * Created by fpin on 7/26/16.
  */
class PartitionColumnFilterTest extends FreeSpec {


  "A PartitionColumnFilter should be correctly build from a string" in {
    assert(PartitionColumnFilter.fromString("Key=Value").get === new PartitionColumnFilter(new PartitionColumn("key","Value"),RangeOperator.Equal))
    assert(PartitionColumnFilter.fromString("Key>Value").get === new PartitionColumnFilter(new PartitionColumn("key","Value"),RangeOperator.Greater))
    assert(PartitionColumnFilter.fromString("Key>=Value").get === new PartitionColumnFilter(new PartitionColumn("key","Value"),RangeOperator.GreaterOrEqual))
    assert(PartitionColumnFilter.fromString("Key<Value").get === new PartitionColumnFilter(new PartitionColumn("key","Value"),RangeOperator.Lower))
    assert(PartitionColumnFilter.fromString("Key<=Value").get === new PartitionColumnFilter(new PartitionColumn("key","Value"),RangeOperator.LowerOrEqual))
  }




}
