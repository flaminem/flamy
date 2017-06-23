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

package com.flaminem.flamy.utils.collection.immutable

import com.flaminem.flamy.utils.AutoClose
import org.scalatest.FreeSpec

/**
  * Created by fpin on 2/16/17.
  */
class AutoCloseTest extends FreeSpec {

  class TestException extends Exception

  import AutoCloseTest._

  "with only one resource" - {

    "AutoClose should work with the for ... syntax" in {
      val resource = AutoClose(new Connection())
      for {
        con <- resource
      } {
        assert(con.isClosed === false)
      }
      val con = resource.resource
      assert(con.isClosed === true)
    }

    "AutoClose should work with the for yield syntax" in {
      val resource = AutoClose(new Connection())

      val wasClosed: Boolean =
        for {
          con <- resource
        } yield {
          assert(con.isClosed === false)
          con.isClosed
        }
      val con = resource.resource
      assert(wasClosed === false)
      assert(con.isClosed === true)
    }

    "AutoClose should work with the for ... syntax with exceptions" in {
      val resource = AutoClose(new Connection())
      intercept[TestException]{
        for {
          con <- resource
        } {
          assert(con.isClosed === false)
          throw new TestException()
        }
      }
      val con = resource.resource
      assert(con.isClosed === true)
    }

    "AutoClose should work with the for yield syntax with exceptions" in {
      val resource = AutoClose(new Connection())
      intercept[TestException]{
        for {
          con <- resource
        } yield {
          assert(con.isClosed === false)
          throw new TestException()
        }
      }
      val con = resource.resource
      assert(con.isClosed === true)
    }

    "AutoClose should work with the for ... syntax and type annotations" in {
      val resource = AutoClose(new Connection())
      for {
        con: Connection <- resource
      } {
        assert(con.isClosed === false)
      }
      val con = resource.resource
      assert(con.isClosed === true)
    }

    "AutoClose should work with the for yield syntax and type annotations" in {
      val resource = AutoClose(new Connection())
      val wasClosed: Boolean =
        for {
          con: Connection <- resource
        } yield {
          assert(con.isClosed === false)
          con.isClosed
        }
      val con = resource.resource
      assert(wasClosed === false)
      assert(con.isClosed === true)
    }

  }

  "with two resources" - {

    "AutoClose should work with the for ... syntax" in {
      val resource = AutoClose(new Connection())
      for {
        con <- resource
        st <- AutoClose(new Statement(con))
      } {
        assert(con.isClosed === false)
        assert(st.isClosed === false)
      }
      val con = resource.resource
      assert(con.isClosed === true)
    }

    "AutoClose should work with the for yield syntax" in {
      val resource = AutoClose(new Connection())
      val wasClosed: (Boolean, Boolean) =
        for {
          con <- resource
          st <- AutoClose(new Statement(con))
        } yield {
          assert(con.isClosed === false)
          assert(st.isClosed === false)
          con.isClosed -> st.isClosed
        }
      val con = resource.resource
      assert(wasClosed === (false, false))
      assert(con.isClosed === true)
    }

    "AutoClose should work with the for ... syntax with exceptions" in {
      val resource = AutoClose(new Connection())
      intercept[TestException]{
        for {
          con <- resource
          st <- AutoClose(new Statement(con))
        } {
          assert(con.isClosed === false)
          assert(st.isClosed === false)
          throw new TestException()
        }
      }
      val con = resource.resource
      assert(con.isClosed === true)
    }

    "AutoClose should work with the for yield syntax with exceptions" in {
      val resource = AutoClose(new Connection())
      intercept[TestException]{
        for {
          con <- resource
          st <- AutoClose(new Statement(con))
        } {
          assert(con.isClosed === false)
          assert(st.isClosed === false)
          throw new TestException()
        }
      }
      val con = resource.resource
      assert(con.isClosed === true)
    }


    "AutoClose should work with the for ... syntax and type annotations" in {
      val resource = AutoClose(new Connection())
      for {
        con: Connection <- resource
        st: Statement <- AutoClose(new Statement(con))
      } {
        assert(con.isClosed === false)
        assert(st.isClosed === false)
      }
      val con = resource.resource
      assert(con.isClosed === true)
    }

    "AutoClose should work with the for yield syntax and type annotations" in {
      val resource = AutoClose(new Connection())
      val wasClosed: (Boolean, Boolean) =
        for {
          con: Connection <- resource
          st: Statement <- AutoClose(new Statement(con))
        } yield {
          assert(con.isClosed === false)
          assert(st.isClosed === false)
          con.isClosed -> st.isClosed
        }
      val con = resource.resource
      assert(wasClosed === (false, false))
      assert(con.isClosed === true)
    }

  }

}


object AutoCloseTest {

  class Connection extends java.lang.AutoCloseable{
    var isClosed = false

    override def close(): Unit = {
      isClosed = true
    }
  }

  class Statement(con: Connection) extends java.lang.AutoCloseable{
    var isClosed = false

    override def close(): Unit = {
      isClosed = true
    }
  }


}