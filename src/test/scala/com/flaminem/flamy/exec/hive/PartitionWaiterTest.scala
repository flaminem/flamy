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
import com.flaminem.flamy.exec.utils.{Action, ActionRunner, ReturnFailure, ReturnSuccess}
import com.flaminem.flamy.model.core.Model
import com.flaminem.flamy.utils.time.TimeUtils
import org.scalatest.FunSpec

/**
  * Created by bdamour on 3/4/16.
  */
class PartitionWaiterTest extends FunSpec {

  def makeWaiter(partitionDescs: Traversable[(String, String, String)]): PartitionWaiter = {
    val context = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/PartitionWaiter")
    val model = Model.getIncompleteModel(context, Nil)
    val fetcher = new DummyHivePartitionFetcher(model, partitionDescs)
    new PartitionWaiter(context, fetcher)
  }

  describe("WaitForPartition") {
    val dBefore: Long = TimeUtils.universalTimeToTimeStamp("2016-03-05 02:24:33")
    val dAfter: Long = TimeUtils.universalTimeToTimeStamp("2016-03-07 02:24:33")
    val dBetween: Long = TimeUtils.universalTimeToTimeStamp("2016-03-06 02:25:33")


    it("should find existing partitions") {
      val waiter = makeWaiter(
        ("db_test.table0/day=2016-03-05", "anyloc", "2016-03-06 02:24:33")
          ::("db_test.table0/day=2016-03-06", "anyloc", "2016-03-07 02:23:33")
          :: Nil
      )
      val items: List[String] = "db_test.table0/day=2016-03-05" :: "db_test.table0/day=2016-03-06" :: Nil
      assertResult(ReturnSuccess)(
        waiter.waitForPartition(items, 1L, None, 1L)
      )
    }
    it("should find existing partitions that appear before the time is out") {
      val context = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/PartitionWaiter")
      val model = Model.getIncompleteModel(context, Nil)
      val fetcher = new DummyHivePartitionFetcher(model, Nil)
      val waiter = new PartitionWaiter(context, fetcher)

      val action = new Action {
        val items: List[String] = "db_test.table0/day=2016-03-05" :: "db_test.table0/day=2016-03-06" :: Nil
        @throws(classOf[Exception])
        override def run(): Unit = {
          Some(waiter.waitForPartition(items, 20L, None, 1L))
        }

        override val name: String = "parallel waitForPartition"
        override val logPath: String = name
      }
      ActionRunner.runInParallel(action)
      Thread.sleep(2000)
      assert(!action.isFinished)
      fetcher.addPartition("db_test.table0/day=2016-03-05", "anyloc", "2016-03-06 02:24:33")
      Thread.sleep(2000)
      assert(!action.isFinished)
      fetcher.addPartition("db_test.table0/day=2016-03-06", "anyloc", "2016-03-07 02:23:33")
      Thread.sleep(2000)
      assert(action.isFinished, "The action was expected to be finished but is not.")

      assert(action.result.get.returnStatus === ReturnSuccess)
    }
    it("should fail to find inexisting partition after the timeout") {
      val waiter = makeWaiter(
        ("db_test.table0/day=2016-03-05", "anyloc", "2016-03-06 02:24:33")
          ::("db_test.table0/day=2016-03-06", "anyloc", "2016-03-07 02:23:33")
          :: Nil
      )
      val timeBefore = System.currentTimeMillis()
      assertResult(ReturnFailure)(
        waiter.waitForPartition("db_test.table0/day=2016-02-18" :: Nil, 3L, None, 1L)
      )
      val timeAfter = System.currentTimeMillis()
      assert(timeAfter - timeBefore >= 3000)
    }
    it("should find partitions with newer modification date") {
      val waiter = makeWaiter(
        ("db_test.table0/day=2016-03-05", "anyloc", "2016-03-06 02:24:33")
          ::("db_test.table0/day=2016-03-06", "anyloc", "2016-03-07 02:23:33")
          :: Nil
      )
      val items: List[String] = "db_test.table0/day=2016-03-05" :: "db_test.table0/day=2016-03-06" :: Nil
      assertResult(ReturnSuccess)(
        waiter.waitForPartition(items, 1L, Some(dBefore), 1L)
      )
    }
    it("should not find partitions with older modification date") {
      val waiter = makeWaiter(
        ("db_test.table0/day=2016-03-05", "anyloc", "2016-03-06 02:24:33")
          ::("db_test.table0/day=2016-03-06", "anyloc", "2016-03-07 02:23:33")
          :: Nil
      )
      val items: List[String] = "db_test.table0/day=2016-03-05" :: "db_test.table0/day=2016-03-06" :: Nil
      assertResult(ReturnFailure)(
        waiter.waitForPartition(items, 1L, Some(dAfter), 1L)
      )
    }
    describe("when one partition has a newer modification date, but others have older modification dates than required") {
      it("should fail to find all the partitions") {
        val waiter = makeWaiter(
          ("db_test.table0/day=2016-03-05", "anyloc", "2016-03-06 02:24:33")
            ::("db_test.table0/day=2016-03-06", "anyloc", "2016-03-07 02:23:33")
            :: Nil
        )
        val items: List[String] = "db_test.table0/day=2016-03-06" :: "db_test.table0/day=2016-03-05" :: Nil
        assertResult(ReturnFailure)(
          waiter.waitForPartition(items, 1L, Some(dBetween), 1L)
        )
      }
    }
  }
}
