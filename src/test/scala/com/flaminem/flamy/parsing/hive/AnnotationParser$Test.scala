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

package com.flaminem.flamy.parsing.hive

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model.Variables
import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.model.partitions.transformation._
import org.scalatest.FreeSpec

/**
  * Created by fpin on 8/15/16.
  */
class AnnotationParser$Test extends FreeSpec {

  implicit val context = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/empty_test")

  "a text with IGNORE partition transformation should be OK" in {
    val text =
      """
        |@regen(
        |  IGNORE db1.source
        |  ;
        |)
        |INSERT OVERWRITE TABLE db2.dest PARTITION(part2)
        |SELECT id, part1
        |FROM db1.source
        |;
      """.stripMargin
    val Seq(actual: Annotation) = AnnotationParser.parseText(text, new Variables(), isView = false)
    assert( actual.isInstanceOf[Ignore] )
    assert( actual.table === TableName("db1.source") )
  }

  "a text with IGNORE TIMESTAMP partition transformation should be OK" in {
    val text =
      """
        |@regen(
        |  IGNORE TIMESTAMP db1.source
        |  ;
        |)
        |INSERT OVERWRITE TABLE db2.dest PARTITION(part2)
        |SELECT id, part1
        |FROM db1.source
        |;
      """.stripMargin
    val Seq(actual: Annotation) = AnnotationParser.parseText(text, new Variables(), isView = false)
    assert( actual.isInstanceOf[IgnoreTimestamp] )
    assert( actual.table === TableName("db1.source") )
  }


}
