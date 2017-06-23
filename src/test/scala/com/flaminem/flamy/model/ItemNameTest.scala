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

import com.flaminem.flamy.model.names.{ItemName, SchemaName, TableName, TablePartitionName}
import org.scalatest.FunSpec

import scala.language.implicitConversions

/**
 * Created by fpin on 12/23/14.
 */
class ItemNameTest extends FunSpec {


  describe("a SchemaName") {
    it("should be recognised from a String"){
      assert(ItemName("db").isInstanceOf[SchemaName])
    }
    it("should not recognize incorrect Strings"){
      intercept[Exception]{ItemName("db.table").asInstanceOf[SchemaName]}
      intercept[Exception]{ItemName("col = true").asInstanceOf[SchemaName]}
    }
    it("should have correct attributes"){
      val schemaName = ItemName("db").asInstanceOf[SchemaName]
      assert(schemaName.name === "db")
      assert(schemaName.fullName === "db")
    }
    it("should have correct membership methods"){
      val schemaName = ItemName("db").asInstanceOf[SchemaName]
      assert(schemaName.isInOrEqual("db"))
      assert(!schemaName.isInOrEqual("db.table"))
      assert(!schemaName.isInOrEqual("db.table/part1=val1/part2=val2"))
      assert(!schemaName.isInOrEqual("toto"))
      assert(!schemaName.isInOrEqual("db.toto"))
      assert(!schemaName.isInOrEqual("db.table/part1=val1/part2=toto"))
    }
  }


  describe("a TableName") {
    val name = "db.table"
    it("should be recognised from a String") {
      val itemName = ItemName(name)
      assert(itemName.isInstanceOf[TableName])
    }
    it("should have correct attributes"){
      val tableName = ItemName(name).asInstanceOf[TableName]
      assert(tableName.name === "table")
      assert(tableName.schemaName.name === "db")
      assert(tableName.fullName === "db.table")
    }
    it("should have correct membership methods"){
      val tableName = ItemName(name).asInstanceOf[TableName]
      assert(tableName.isInSchema("db"))
      assert(!tableName.isInSchema("toto"))
      assert(tableName.isInOrEqual("db"))
      assert(tableName.isInOrEqual("db.table"))
      assert(!tableName.isInOrEqual("db.table/part1=val1/part2=val2"))
      assert(!tableName.isInOrEqual("toto"))
      assert(!tableName.isInOrEqual("db.toto"))
      assert(!tableName.isInOrEqual("db.table/part1=val1/part2=toto"))
    }
  }


  describe("a TablePartitionName") {
    val name = "db.table/part1=val1/part2=val2"
    it("should be recognised from a String"){
      val itemName = ItemName(name)
      assert(itemName.isInstanceOf[TablePartitionName])
    }
    it("should have correct attributes"){
      val partitionName = ItemName(name).asInstanceOf[TablePartitionName]
      assert(partitionName.tableName.fullName=== "db.table")
      assert(partitionName.tableName.name === "table")
      assert(partitionName.schemaName.name === "db")
      assert(partitionName.partitionName === "part1=val1/part2=val2")
    }
    it("should have correct membership methods"){
      val partitionName = ItemName(name).asInstanceOf[TablePartitionName]
      assert(partitionName.isInSchema("db"))
      assert(!partitionName.isInSchema("toto"))
      assert(partitionName.isInTable("db.table"))
      assert(!partitionName.isInTable("db"))
      assert(!partitionName.isInTable("db.toto"))
      assert(!partitionName.isInTable("toto.table"))
      assert(partitionName.isInOrEqual("db"))
      assert(partitionName.isInOrEqual("db.table"))
      assert(partitionName.isInOrEqual("db.table/part1=val1/part2=val2"))
      assert(!partitionName.isInOrEqual("toto"))
      assert(!partitionName.isInOrEqual("db.toto"))
      assert(!partitionName.isInOrEqual("db.table/part1=val1/part2=toto"))
    }
  }



  }
