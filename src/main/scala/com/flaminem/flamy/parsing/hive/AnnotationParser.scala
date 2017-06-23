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
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.model.Variables
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.model.partitions.transformation.Annotation

import scala.util.control.NonFatal

/**
  *
  */
object AnnotationParser {

  @throws(classOf[FlamyException])
  def parseText(text: String, vars: Variables, isView: Boolean)(implicit context: FlamyContext): Seq[Annotation] = {
    val annotations = QueryUtils.findAnnotations(QueryUtils.cleanQuery(text))
    annotations.flatMap{
      case ("regen", query) =>
        try {
          QueryUtils.splitQuery(query).flatMap{parseQuery(_, vars, isView)}
        } catch {
          case e: FlamyParsingException if e.query.isDefined => throw e
          case NonFatal(e) => throw FlamyParsingException(query, e, verbose = true)
        }
      case (name, query) =>
        FlamyOutput.out.warn(s"Only annotations of type 'regen' are supported yet. Got $name")
        None
    }
  }

  private[hive] def parseQuery(query: String, vars: Variables, isView: Boolean)(implicit context: FlamyContext): Seq[Annotation] = {
    if(isView){
      throw new FlamyParsingException("Annotations are not allowed in views.")
    }
    else{
      Annotation(query)::Nil
    }
  }

}

