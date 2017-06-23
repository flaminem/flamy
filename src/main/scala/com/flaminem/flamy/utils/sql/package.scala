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

package com.flaminem.flamy.utils


import java.sql.ResultSet

import com.flaminem.flamy.model.Column
import com.flaminem.flamy.utils.sql.hive.{CachedResultSet, StreamedResultSet}

/**
* Created by fpin on 12/15/14.
*/
package object sql {


  implicit class ResultSetExtension(resultSet: ResultSet) {

    val metaData: MetaData = {
      val md = resultSet.getMetaData
      new MetaData((1 to md.getColumnCount).map{ i => new Column(md.getColumnLabel(i), md.getColumnTypeName(i)) })
    }

    def currentData: Seq[String] = {
      (1 to metaData.size).map{i => resultSet.getString(i)}
    }

    //noinspection ScalaStyle
    private def iterator = new Iterator[ResultRow] {
      var hasNextResult: Option[Boolean] = None
      var currentRow: Option[ResultRow] = None

      override def hasNext: Boolean = {
        if(hasNextResult.isEmpty) {
          if(resultSet.next()) {
            hasNextResult = Some(true)
            currentRow = Some(new ResultRow(metaData, currentData))
          }
          else {
            hasNextResult = Some(false)
            currentRow = None
          }
        }
        hasNextResult.get
      }

      override def next: ResultRow = {
        if(currentRow.isEmpty) {
          hasNext
        }
        val row = currentRow.get
        currentRow = None
        hasNextResult = None
        row
      }
    }

    /**
      * Store the entire ResultSet in cache.
      * This allows for easier data handling using scala collections.
      * All the columns are stored in the cache as Strings.
      * Beware that if your table is big, you might have a big latency to initialize the cache, or even end up with an OutOfMemoryException.
      * In such case, you might prefer the [[stream]] method.
      * Warning: Using this method will deplete this ResultSet.
      * @return
      */
    def cache: CachedResultSet = new CachedResultSet(metaData, iterator.toSeq)

    /**
      * Get a stream to iterate on the ResultSet.
      * This allows for easier data handling using scala collections.
      * All the columns are stored in the cache as Strings.*
      * This provides a more memory efficient way to iterate over the results,
      * but it also allows you to do only one pass.
      * Warning: Using this method and iterating over the result will deplete this ResultSet.
      * @return
      */
    def stream: StreamedResultSet = new StreamedResultSet(metaData, iterator)

  }


}
