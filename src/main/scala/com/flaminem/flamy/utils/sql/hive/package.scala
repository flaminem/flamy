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

package com.flaminem.flamy.utils.sql

import java.util

import com.flaminem.flamy.model.Column
import org.apache.hadoop.hive.ql.Driver

import scala.collection.JavaConversions._

/**
  * Created by fpin on 8/7/16.
  */
package object hive {



  implicit class DriverExtension(driver: Driver) {

    val metaData: MetaData = {
      new MetaData(driver.getSchema.getFieldSchemas.toSeq.map{new Column(_)})
    }

    private def iterator = new Iterator[Seq[ResultRow]] {
      val res: util.ArrayList[String] = new util.ArrayList[String]()
      override def hasNext: Boolean = driver.getResults(res)
      override def next(): Seq[ResultRow] = res.map{row => new ResultRow(metaData, row.split("\t"))}
    }

    /**
      * Store the entire driver's results in cache.
      * This allows for easier data handling using scala collections.
      * All the columns are stored in the cache as Strings.
      * Beware that if your table is big, you might have a big latency to initialize the cache, or even end up with an OutOfMemoryException.
      * In such case, you might prefer the [[stream]] method.
      * Warning: Using this method will deplete this driver's results.
      * @return
      */
    def cache: CachedResultSet = new CachedResultSet(metaData, iterator.flatten.toSeq)

    /**
      * Get a stream to iterate on this driver's results.
      * This allows for easier data handling using scala collections.
      * All the columns are stored in the cache as Strings.*
      * This provides a more memory efficient way to iterate over the results,
      * but it also allows you to do only one pass.
      * Warning: Using this method and iterating over the result will deplete this driver's results.
      * @return
      */
    def stream: StreamedResultSet = new StreamedResultSet(metaData, iterator.flatten)

  }


}
