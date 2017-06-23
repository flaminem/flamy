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
  * Created by fpin on 7/27/16.
  */
//noinspection SimplifyBoolean
class RangeOperatorTest extends FreeSpec {


  "Equal should work" in {
    assert(RangeOperator.Equal(new PartitionColumn("day","2015-05-06"), new PartitionColumn("day","2015-05-07")) == false)
    assert(RangeOperator.Equal(new PartitionColumn("day","2015-05-07"), new PartitionColumn("day","2015-05-07")) == true)
    assert(RangeOperator.Equal(new PartitionColumn("day","2015-05-08"), new PartitionColumn("day","2015-05-07")) == false)
  }

  "Lower should work" in {
    assert(RangeOperator.Lower(new PartitionColumn("day","2015-05-06"), new PartitionColumn("day","2015-05-07")) == true)
    assert(RangeOperator.Lower(new PartitionColumn("day","2015-05-07"), new PartitionColumn("day","2015-05-07")) == false)
    assert(RangeOperator.Lower(new PartitionColumn("day","2015-05-08"), new PartitionColumn("day","2015-05-07")) == false)
  }

  "LowerOrEqual should work" in {
    assert(RangeOperator.LowerOrEqual(new PartitionColumn("day","2015-05-06"), new PartitionColumn("day","2015-05-07")) == true)
    assert(RangeOperator.LowerOrEqual(new PartitionColumn("day","2015-05-07"), new PartitionColumn("day","2015-05-07")) == true)
    assert(RangeOperator.LowerOrEqual(new PartitionColumn("day","2015-05-08"), new PartitionColumn("day","2015-05-07")) == false)
  }

  "Greater should work" in {
    assert(RangeOperator.Greater(new PartitionColumn("day","2015-05-06"), new PartitionColumn("day","2015-05-07")) == false)
    assert(RangeOperator.Greater(new PartitionColumn("day","2015-05-07"), new PartitionColumn("day","2015-05-07")) == false)
    assert(RangeOperator.Greater(new PartitionColumn("day","2015-05-08"), new PartitionColumn("day","2015-05-07")) == true)
  }

  "GreaterOrEqual should work" in {
    assert(RangeOperator.GreaterOrEqual(new PartitionColumn("day","2015-05-06"), new PartitionColumn("day","2015-05-07")) == false)
    assert(RangeOperator.GreaterOrEqual(new PartitionColumn("day","2015-05-07"), new PartitionColumn("day","2015-05-07")) == true)
    assert(RangeOperator.GreaterOrEqual(new PartitionColumn("day","2015-05-08"), new PartitionColumn("day","2015-05-07")) == true)
  }




}
