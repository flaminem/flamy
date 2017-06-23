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

import scala.collection.IndexedSeq
import scala.collection.mutable.ListBuffer

/**
  * Contain util functions to compute the diff between collections
  */
object DiffUtils {

  private def hamming[T](s1: IndexedSeq[T], s2: IndexedSeq[T], allowReplacements: Boolean): Array[Array[Int]] = {
    val l1 = s1.length
    val l2 = s2.length

    val work: Array[Array[Int]] = Array.ofDim(l1 + 1, l2 + 1)

    work(0)(0) = 0

    var i = 1
    var j = 1

    /* in scala, while loops are slightly faster than for loops (scala 2.10 at least) */
    while( i <= l1 ) {
      work(i)(0) = i
      i += 1
    }
    while( j <= l2 ) {
      work(0)(j) = j
      j += 1
    }

    i = 0
    while( i < l1 ){
      j = 0
      while (j < l2) {
        val d = math.min(1 + work(i + 1)(j), 1 + work(i)(j + 1))
        if (s1(i) == s2(j)) {
          work(i + 1)(j + 1) = math.min(work(i)(j), d)
        }
        else if(allowReplacements) {
          work(i + 1)(j + 1) = math.min(work(i)(j) + 1, d)
        }
        else {
          work(i + 1)(j + 1) = d
        }
        j += 1
      }
      i += 1
    }
    work
  }

  /**
    * Compute the hamming distance between two sequences.
    * Addition, removal, and substitution count as 1.
    * @param s1
    * @param s2
    * @param allowReplacements true if replacements should be allowed
    * @tparam T
    * @return
    */
  def hammingDistance[T](s1: IndexedSeq[T], s2: IndexedSeq[T], allowReplacements: Boolean): Int = {
    val l1 = s1.length
    val l2 = s2.length
    val work = hamming(s1, s2, allowReplacements)
    work(l1)(l2)
  }

  /**
    * From a given pair of <code>IndexedSeq[T]<code> </code>(s1, s2)</code>,
    * compute the corresponding pair of <code>IndexedSeq[Option[T]]</code> <code>(l1, l2)</code>,
    * such that l1 and l2 have the same length and <code>s1 == l1.flatten</code> and <code>s1 == l2.flatten</code>
    * and the number of pairwise elements of <code>l1</code> and <code>l2</code> that are different
    * is minimal, and equal to the Hamming distance between <code>s1</code> and <code>s2</code>).
    * @param s1
    * @param s2
    * @param allowReplacements true if replacements should be allowed
    * @tparam T
    * @return
    */
  def hammingDiff[T](s1: IndexedSeq[T], s2: IndexedSeq[T], allowReplacements: Boolean): (IndexedSeq[(Option[T],Option[T])]) = {
    val l1 = s1.length
    val l2 = s2.length
    val work = hamming(s1, s2, allowReplacements)
    val sb1 = new ListBuffer[Option[T]]()
    val sb2 = new ListBuffer[Option[T]]()

    var i = l1-1
    var j = l2-1

    /* in scala, while loops are slightly faster than for loops (scala 2.10 at least) */
    while( i >= 0 && j >= 0 ) {
      work(i+1)(j+1) match {
        case w if work(i)(j) == w && s1(i) == s2(j) =>
          sb1 += Some(s1(i))
          sb2 += Some(s2(j))
          i -= 1
          j -= 1
        case w if work(i+1)(j) == w - 1 =>
          sb1 += None
          sb2 += Some(s2(j))
          j -= 1
        case w if work(i)(j+1) == w - 1 =>
          sb1 += Some(s1(i))
          sb2 += None
          i -= 1
        case w if work(i)(j) == w - 1 && s1(i) != s2(j) && allowReplacements =>
          sb1 += Some(s1(i))
          sb2 += Some(s2(j))
          i -= 1
          j -= 1
        case w => throw new IllegalStateException("this should not happen")
      }
    }
    while( i >= 0 ) {
      sb1 += Some(s1(i))
      sb2 += None
      i -= 1
    }
    while( j >= 0 ) {
      sb1 += None
      sb2 += Some(s2(j))
      j -= 1
    }
    sb1.view.reverse.zip(sb2.view.reverse).toIndexedSeq.view.force
  }

}
