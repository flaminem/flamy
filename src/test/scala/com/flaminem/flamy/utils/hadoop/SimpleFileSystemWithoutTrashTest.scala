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

package com.flaminem.flamy.utils.hadoop

import org.apache.hadoop.fs.Trash
import org.scalatest.FunSpec

/**
 * Created by fpin on 2/10/15.
 */
class SimpleFileSystemWithoutTrashTest extends FunSpec with FileSystemWithoutTrashTesting {

  var sfs: SimpleFileSystem = null ;

  override def beforeEach() = {
    super.beforeEach
    sfs = new SimpleFileSystem(fs) ;
  }

  val testDir = f"$testRootDir/dir"
  val testSubDir = f"$testRootDir/dir/subDir"

  describe("for a FileSystemExtensions without a Trash") {

    describe("trash") {
      it("should be disabled"){
        val trash = new Trash(fs.getConf)
        assert(!trash.isEnabled)
      }
    }

    describe("mkdirs and delete") {
      it("should work") {
        assert(!fs.exists(testDir))
        assert(fs.mkdirs(testDir))
        assert(fs.exists(testDir))
        assert(fs.delete(testDir,true))
        assert(!fs.exists(testDir))
      }
    }

    describe("moveToTrash") {
      it("should fail") {
        assert(!fs.exists(testDir))
        assert(fs.mkdirs(testDir))
        assert(fs.exists(testDir))
        assert(!sfs.moveToTrash(testDir))
        assert(fs.exists(testDir))
      }
    }

    describe("makeEmptyFolder") {
      it("should create an empty directory if it does not exist") {
        assert(!fs.exists(testDir))
        assert(sfs.makeEmptyFolder(testDir))
        assert(fs.exists(testDir))
        assert(fs.listStatus(testDir).length===0)
      }

      ignore("should remove the directory and create a new one if it does already exist") {
        assert(!fs.exists(testDir))
        assert(fs.mkdirs(testDir))
        assert(fs.mkdirs(testSubDir))
        assert(fs.listStatus(testDir).length===1)
        assert(sfs.makeEmptyFolder(testDir))
        assert(fs.exists(testDir))
        assert(fs.listStatus(testDir).length===0)
      }
    }


  }




}
