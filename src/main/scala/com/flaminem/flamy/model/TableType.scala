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

package com.flaminem.flamy.model

/**
  * Created by fpin on 8/24/16.
  */
sealed trait TableType {

}

object TableType {

  /** Table with its info */
  case object TABLE extends TableType

  /** View with its info */
  case object VIEW extends TableType

  /** Temporary table used for subqueries */
  case object TEMP extends TableType

  /** Temporary table representing a lateral view */
  case object LATERAL_VIEW extends TableType

  /** Reference to a (not yet) analyzed tableDependency */
  case object REF extends TableType

  /** External tableDependency (from META.properties) */
  case object EXT extends TableType

  /** Table definition from a remote metastore: TableType = null */
  case object REMOTE extends TableType

}
