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

import com.flaminem.flamy.model.names.ItemName

/**
  * This trait represent the items given as argument to flamy.
  * This can be either a range (with options --from and --to) or a list of items.
  */
sealed trait ItemArgs {

  /**
    * @return the sequence of all items given as argument
    */
  def allItems: Seq[ItemName]

  /**
    * @return true if this contains no item
    */
  def isEmpty: Boolean = {
    allItems.isEmpty
  }

}

case class ItemList(items: Seq[ItemName]) extends ItemArgs{

  override def allItems: Seq[ItemName] = items

}

case class ItemRange(from: Seq[ItemName], to: Seq[ItemName]) extends ItemArgs{

  override def allItems: Seq[ItemName] = from++to

}

object ItemArgs {

  /**
    * Build a new ItemArgs from the command line options.
    * If items is nonempty, from and to should be empty.
    * If all items are empty, return an empty ItemList.
    * @param items
    * @param from
    * @param to
    * @return
    */
  def apply(items: Seq[ItemName], from: Seq[ItemName], to: Seq[ItemName]): ItemArgs = {
    if(items.isEmpty){
      if(from.isEmpty || to.isEmpty){
        assert(from.isEmpty && to.isEmpty, "Cannot use --from without --to")
        ItemList(Nil)
      }
      else {
        ItemRange(from, to)
      }
    }
    else {
      assert(from.isEmpty && to.isEmpty, "Cannot use --from and --to and also provide a list of items")
      ItemList(items)
    }
  }

}




