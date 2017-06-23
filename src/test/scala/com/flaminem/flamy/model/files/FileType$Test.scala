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

package com.flaminem.flamy.model.files

import org.scalatest.FunSuite

class FileType$Test extends FunSuite {

  test("FileType.values should work") {
    assert(FileType.values.size==7)
  }

  test("CREATE.matchesFileName should work") {
    assert(FileType.CREATE.matchesFileName("CREATE.hql"))
    assert(!FileType.CREATE.matchesFileName("CREATE123.hql"))
    assert(!FileType.CREATE.matchesFileName("CREATE_123.hql"))
  }

  test("POPULATE.matchesFileName should work") {
    assert(FileType.POPULATE.matchesFileName("POPULATE.hql"))
    assert(FileType.POPULATE.matchesFileName("POPULATE_123.hql"))
    assert(!FileType.POPULATE.matchesFileName("POPULATE123.hql"))
  }

  test("FileType.getTypeFromFileName should work") {
    assert(FileType.getTypeFromFileName("CREATE.hql") === Some(FileType.CREATE))
    assert(FileType.getTypeFromFileName("POPULATE.hql") === Some(FileType.POPULATE))
    assert(FileType.getTypeFromFileName("POPULATE123.hql") === None)
    assert(FileType.getTypeFromFileName("POPULATE_123.hql") === Some(FileType.POPULATE))
  }

  test("FileType.getTitleFromFileName should work") {
    assert(FileType.POPULATE.getTitleFromFileName("POPULATE_123.hql") === Some("123"))
    assert(FileType.POPULATE.getTitleFromFileName("POPULATE.hql") === None)
    assert(FileType.POPULATE.getTitleFromFileName("POPULATE123.hql") === None)
  }

}
