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
import com.flaminem.flamy.exec.utils.{ReturnFailure, ReturnStatus, ReturnSuccess, Waiter}
import com.flaminem.flamy.model.names.{ItemName, TablePartitionName}
import com.flaminem.flamy.utils.logging.Logging
import com.flaminem.flamy.utils.time.TimeUtils

import scala.collection.immutable.Seq

/**
  * Created by fpin on 12/22/14.
  */
class PartitionWaiter(context: FlamyContext, fetcher: HivePartitionFetcher) extends Logging with AutoCloseable{

  def this(context: FlamyContext) {
    this(context, HivePartitionFetcher(context))
  }
  
  /**
    *
    * @param args          should be a list of partitions names formatted as such: "schema.table/key=value/k2=v2/../kn=vn"
    * @param timeout       in seconds
    * @param after         TIMESTAMP
    * @param retryInterval in seconds
    * @return
    */
  def waitForPartition(args: List[ItemName], timeout: Long, after: Option[Long], retryInterval: Long): ReturnStatus = {
    // parse args into a list of TablePartitionName
    if (args.size < 1) {
      throw new IllegalArgumentException(f"Wrong number of arguments : Expecting a list of fully qualified partition" +
        f" name: 'schema.table/key=value/k2=v2/../kn=vn'")
    }
    val partitions: Seq[TablePartitionName] =
      args.map{
        case p: TablePartitionName => p
        case other =>
          throw new IllegalArgumentException(f"Wrong argument ($other) : Expecting a fully qualified partition name: " +
            f"'schema.table/key=value/k2=v2/../kn=vn'")
      }
    waitForPartition(partitions, timeout, after, retryInterval)
  }

  private def partitionsExist(partitions: Seq[TablePartitionName], after: Option[Long]): Boolean = {
    logger.info(
      s"waiting for partitions: ${partitions.mkString(", ")} ; " +
        s"with modification time after ${TimeUtils.timestampToUniversalTime(after.getOrElse(0L))}"
    )
    partitions.forall {
      p =>
        fetcher.getPartition(p) match {
          case None =>
            logger.info(s"partition does not exist yet: $p")
            false
          case Some(part) =>
            val modificationTime = part.getModificationTime(context, refresh = false).getOrElse(0L)
            if( modificationTime >= after.getOrElse(0L) ) {
              logger.info(s"partition found: $p - modified: ${TimeUtils.timestampToUniversalTime(modificationTime)}")
              true
            }
            else {
              val mt = TimeUtils.timestampToUniversalTime(modificationTime)
              val at = TimeUtils.timestampToUniversalTime(after.getOrElse(0L))
              logger.info(s"partition found, but waiting for it to be regenerated: $p modification time is $mt, expected to be greater than $at")
              false
            }
        }
    }
  }

  /**
    * Wait until all partitions are found or fail after timeout
    *
    * @param partitions
    * @param after         TIMESTAMP
    * @param retryInterval in seconds
    * @return
    */
  private def waitForPartition(partitions: Seq[TablePartitionName], timeout: Long, after: Option[Long], retryInterval: Long): ReturnStatus = {
    Waiter.waitForEvent(partitionsExist(partitions, after), timeout, retryInterval) match {
      case true => ReturnSuccess
      case false => ReturnFailure
    }
  }

  override def close(): Unit = {
    this.fetcher.close()
  }

}
