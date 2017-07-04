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

package com.flaminem.flamy.conf

import com.flaminem.flamy.utils.prettyprint.Tabulator

import scala.language.existentials
import scala.util.{Failure, Success, Try}

/**
 * Created by fpin on 4/22/15.
 */
class FlamyContextFormatter(context: FlamyContext) {

  def format(): String = {
    val confVars: Seq[ConfVarTemplate[_]] = context.confVars ++ FlamyGlobalContext.confVars
    val header: Seq[Any] =
      Seq(
         "level"
        ,"property"
        ,"value"
        ,"validator"
//        ,"default"
//        ,"description"
      )
    val table: Seq[Seq[Any]] =
      confVars.flatMap{
        case confVar =>
          Try(confVar.getStringFormattedProperty) match {
            case Success(v) =>
              (
                (confVar.confLevel, confVar.propertyKey),
                Seq(
                  confVar.confLevel
                  ,confVar.propertyKey
                  ,v
                  ,confVar.validator.name(confVar.defaultValue.isDefined)
                  //                ,confVar.defaultValue
                  //                ,confVar.validator.name
                  //                ,confVar.description
                )
              ) :: Nil
            case Failure(e) =>
              Nil
          }
      }.toSeq.sortBy{_._1._2}.sortBy{_._1._1}.map{_._2}
    new Tabulator(leftJustify = true).format(header+:table)
  }

}
