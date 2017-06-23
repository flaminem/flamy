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

package com.flaminem.flamy.utils.ordering

/**
  * Created by fpin on 8/19/16.
  */
object OrderingUtils {

  /**
    * Find the first element not equal to zero.
    * If all elements are zero, returns zero.
    * Elements are lazy, this is useful for defining orderings simply.
    */
  def firstNonZero(
    elem1: => Int,
    elem2: => Int
  ): Int = {
    if(elem1 != 0){
      elem1
    }
    else{
      elem2
    }
  }

  /**
    * Find the first element not equal to zero.
    * If all elements are zero, returns zero.
    * Elements are lazy, this is useful for defining orderings simply.
    */
  def firstNonZero(
    elem1: => Int,
    elem2: => Int,
    elem3: => Int
  ): Int = {
    if(elem1 != 0){
      elem1
    }
    else if(elem2 != 0) {
      elem2
    }
    else{
      elem3
    }
  }

  /**
    * Find the first element not equal to zero.
    * If all elements are zero, returns zero.
    * Elements are lazy, this is useful for defining orderings simply.
    */
  def firstNonZero(
    elem1: => Int,
    elem2: => Int,
    elem3: => Int,
    elem4: => Int
  ): Int = {
    if(elem1 != 0){
      elem1
    }
    else if(elem2 != 0) {
      elem2
    }
    else if(elem3 != 0) {
      elem3
    }
    else{
      elem4
    }
  }

  /**
    * Find the first element not equal to zero.
    * If all elements are zero, returns zero.
    * Elements are lazy, this is useful for defining orderings simply.
    */
  def firstNonZero(
    elem1: => Int,
    elem2: => Int,
    elem3: => Int,
    elem4: => Int,
    elem5: => Int
  ): Int = {
    if(elem1 != 0){
      elem1
    }
    else if(elem2 != 0) {
      elem2
    }
    else if(elem3 != 0) {
      elem3
    }
    else if(elem4 != 0) {
      elem4
    }
    else{
      elem5
    }
  }

}
