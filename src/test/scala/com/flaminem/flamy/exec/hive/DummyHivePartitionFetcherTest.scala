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

package com.flaminem.flamy.exec.hive

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model.core.Model
import com.flaminem.flamy.model.exceptions.FlamyException
import org.scalatest.FunSpec

/**
  * Created by fpin on 5/26/16.
  */
class DummyHivePartitionFetcherTest extends FunSpec {

  val context = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/DummyHivePartitionFetcher")
  val model: Model = Model.getCompleteModel(context, Nil)

  describe("a DummyHivePartitionFetcher") {
    describe("when the partitioning info is not consistent with the table partition definition") {
      it("should throw a FlamyException") {
        intercept[FlamyException]{
          val fetcher = new DummyHivePartitionFetcher(model, Seq(("less_partition_columns.table1/part2=A", "anyloc", "2000-01-01 02:22:33")))
          fetcher.getTablePartitioningInfo("less_partition_columns.table1")
        }
      }
    }
    describe("when the partitioning info is not consistent with the table partition definition bis") {
      it("should throw a FlamyException") {
        intercept[FlamyException]{
          val fetcher = new DummyHivePartitionFetcher(model, Seq(("more_partition_columns.table2/part1=A", "anyloc", "2000-01-01 02:22:33")))
          fetcher.getTablePartitioningInfo("more_partition_columns.table2")
        }
      }
    }
    describe("when the partitioning info is not consistent with the table partition definition ter") {
      it("should throw a FlamyException") {
        intercept[FlamyException]{
          val fetcher = new DummyHivePartitionFetcher(model, Seq(("more_partition_columns.table2/part2=B", "anyloc", "2000-01-01 02:22:33")))
          fetcher.getTablePartitioningInfo("more_partition_columns.table2")
        }
      }
    }
    describe("when the partitioning info is consistent") {
      it("should be ok") {
        val fetcher = new DummyHivePartitionFetcher(model, Seq(("more_partition_columns.table2/part1=A/part2=B", "anyloc", "2000-01-01 02:22:33")))
        fetcher.getTablePartitioningInfo("more_partition_columns.table2")
      }
    }
    describe("when the partitioning info is consistent but the partition order is different") {
      it("should be ok") {
        val fetcher = new DummyHivePartitionFetcher(model, Seq(("more_partition_columns.table2/part2=B/part1=A", "anyloc", "2000-01-01 02:22:33")))
        fetcher.getTablePartitioningInfo("more_partition_columns.table2")
      }
    }
  }

}
