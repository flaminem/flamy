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

import java.util.regex.Pattern

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model.PartitionKey
import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.utils.prettyprint.Tabulator

import scala.collection.mutable.ListBuffer
import scala.collection.{SeqLike, mutable}


/**
 * Contains all the info about a table's partitioning
 */
class TablePartitioningInfo(
  val tableName: TableName,
  val partitionKeys: Seq[PartitionKey],
  val tablePartitions: Seq[PartitionWithInfo]
)
extends Seq[PartitionWithInfo]
with SeqLike[PartitionWithInfo, TablePartitioningInfo] {

  override def newBuilder: mutable.Builder[PartitionWithInfo, TablePartitioningInfo] = {
    new ListBuffer[PartitionWithInfo].mapResult{x => new TablePartitioningInfo(tableName, partitionKeys, x)}
  }
  override def length: Int = tablePartitions.size
  override def iterator: Iterator[PartitionWithInfo] = tablePartitions.iterator

  lazy val sortedTablePartitions: Seq[PartitionWithInfo] = {
    implicit val ord = new Ordering[PartitionWithInfo] {
      override def compare(x: PartitionWithInfo, y: PartitionWithInfo): Int =
        x.compare(y)
    }
    tablePartitions.sorted[PartitionWithInfo]
  }

  type Key = String
  type Value = String


  /**
    * Getting the modificationTime of each partition is costly.
    * By using this method first, the FileStatus of all partitions
    * will be refreshed in one batch call.
    * You can then call the getModificationTime(context, refresh = false)
    * on each partition without any performance penalty
    * @param context
    * @return this
    */
  def refreshAllFileStatus(context: FlamyContext): TablePartitioningInfo = {
//    val pathToPartitionMap: Map[String, TPartitionWithInfo] = tablePartitions.map{p => new Path(p.location).toString -> p}.toMap
//    val pathsGroupedByScheme: Map[String, Array[Path]] =
//      pathToPartitionMap.keys.map{new Path(_).getParent}.toArray.distinct.groupBy{_.toUri.getScheme}
//    try {
//      for {
//        (scheme, paths) <- pathsGroupedByScheme
//        fs: SimpleFileSystem = context.getFileSystem(scheme)
//        status <- fs.fileSystem.listStatus(paths)
//        partition <- pathToPartitionMap.get(status.getPath.toString)
//      } {
//        partition.fileStatus.update(Some(status))
//      }
//    }
//    catch {
//      case NonFatal(e) => e.printStackTrace()
//    }
    this
  }

  def partitionColumnNames: Seq[String] = partitionKeys.map{_.columnName}

  def timePattern: Pattern = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}")

  private def detectTime(values: Traversable[String]): Option[Boolean] =
    values.count{v => timePattern.matcher(v).matches()} match {
      case 0 => None
      case n if n==values.size => Some(true)
      case n if 100.0*n/values.size>=50 => Some(false)
      case _ => None
    }

  private lazy val analyzedSlice: Seq[(Key, Traversable[Value], Option[Boolean])] = {
    keys.zip(values.transpose.toSeq).map{case (key, values) => (key,values,detectTime(values))}
  }

  lazy val timedSlices: Seq[(Key, Traversable[Value])] = {
    analyzedSlice.flatMap{
      case (key,values,Some(true)) => Some(key, values)
      case _ => None
    }
  }

  lazy val badTimedSlice: Seq[(Key, String)] = {
    analyzedSlice.flatMap{
      case (key,values,Some(false)) =>
        Some((key,"Some values of this partition are dates, but not all of them."))
      case _ => None
    }
  }

  /**
   * Returns the column number of a partition.
   *
   * @param partColName
   * @return
   */
  private def partitionColumnNumber(partColName: String): Int = keys.indexWhere(_.equalsIgnoreCase(partColName))

  def keys: Seq[Key] = partitionKeys.map{_.columnName}

  private def values: Traversable[Seq[Value]] = tablePartitions.map{_.values}

  /**
   * the partition values for the given partition column number
   *
   * @param partColNum
   * @return
   */
  private def partitionValues(partColNum: Int): Traversable[String] = values.map{_(partColNum)}

  /**
   * Returns the partition values for the given partition
   *
   * @param partColName
   * @return
   */
  private def partitionValues(partColName: String): Traversable[String] = {
    partitionColumnNumber(partColName) match {
      case -1 => Seq()
      case i => partitionValues(i)
    }
  }

  def toFormattedString(context: FlamyContext, humanReadable: Boolean): String = {
    val header: Seq[String] = keys ++ Seq("size", "num_files", "modification_time", "format")
    val table: Traversable[Seq[Any]] = sortedTablePartitions.map { p => p.values ++ p.getFormattedInfo(context, humanReadable) }
    assert(table.isEmpty || header.size == table.head.size,
      s"""The header and the first row of the table do not have the same size.
         |Header is: $header
         |First table row is: ${table.head}""".stripMargin)
    Tabulator.format(Seq(header)++table)
  }

  override def apply(idx: Int): PartitionWithInfo = {
    tablePartitions(idx)
  }

  def get(tablePartition: TPartition): Option[PartitionWithInfo] = {
    tablePartitions.find{_==tablePartition}
  }

  override def toString(): String = f"${this.getClass.getSimpleName}($tableName,$tablePartitions)"

}

