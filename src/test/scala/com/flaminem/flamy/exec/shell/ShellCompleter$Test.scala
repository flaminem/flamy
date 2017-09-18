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

package com.flaminem.flamy.exec.shell

import com.flaminem.flamy.conf.FlamyContext
import org.scalatest.FreeSpec

import scala.collection.JavaConversions._

class ShellCompleter$Test extends FreeSpec {

  val context: FlamyContext =
    new FlamyContext(
      "flamy.model.dir.paths" -> "src/test/resources/test",
      "flamy.env.dev.hive.meta.fetcher" -> "direct",
      "flamy.env.test.hive.meta.fetcher" -> "direct"
    )

  context.dryRun = true
  val shellCompleter: ShellCompleter = new ShellCompleter(context)

  "ShellCompleter should complete command: " - {
    "empty" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("", 0, candidates)
      assert(index === 0)
      assert(candidates.contains("help"))
      assert(candidates.contains("version"))
      assert(candidates.contains("show"))
      assert(candidates.contains("run"))
      assert(candidates.contains("diff"))
      assert(candidates.contains("push"))
    }

    "sho" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("sho", 3, candidates)
      assert(index === 0)
      assert(candidates.toSeq === Seq("show"))
    }

    "show" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("show", 4, candidates)
      assert(index === 0)
      assert(candidates.toSeq === Seq("show"))
    }

    "show (with space)" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("show ", 5, candidates)
      assert(index === 5)
      println(candidates)
      assert(candidates.contains("schemas"))
      assert(candidates.contains("tables"))
      assert(candidates.contains("graph"))
    }

    "show (with index in the middle)" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("show", 3, candidates)
      assert(index === 0)
      assert(candidates.toSeq === Seq("show"))
    }

    "--conf flamy.exec.parallelism=2 show tab" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("--conf flamy.exec.parallelism=2 show tab", 40, candidates)
      assert(index === 37)
      assert(candidates.toSeq === Seq("tables"))
    }

    "describe tables --" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("describe tables --", 18, candidates)
      assert(index === 16)
      assert(candidates.toSeq === Seq("--bytes", "--on"))
    }

    "describe tables --on " in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("describe tables --on ", 21, candidates)
      assert(index === 21)
      assert(candidates.toSeq === Seq("dev", "test"))
    }

    "describe tables --on d" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("describe tables --on d", 22, candidates)
      assert(index === 21)
      assert(candidates.toSeq === Seq("dev"))
    }

    "describe tables --on dev " in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("describe tables --on dev ", 25, candidates)
      assert(index === 25)
      assert(candidates.toSeq === Seq("db_dest", "db_source", "db_test"))
    }

    "show tables db_dest " in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("show tables db_dest ", 20, candidates)
      assert(index === 20)
      assert(candidates.toSeq === Seq("db_source", "db_test"))
    }

    "show tables db_dest db_" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("show tables db_dest db_", 23, candidates)
      assert(index === 20)
      assert(candidates.toSeq === Seq("db_source", "db_test"))
    }

    "show tables db_source.source db_" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("show tables db_source.source db_", 32, candidates)
      assert(index === 29)
      assert(candidates.toSeq === Seq("db_dest", "db_source", "db_test"))
    }

    "show tables db_source.source db_source." in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("show tables db_source.source db_source.", 39, candidates)
      assert(index === 39)
      assert(candidates.toSeq === Seq("source_view"))
    }

    "show tables --on dev db_dest " in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("show tables --on dev db_dest ", 29, candidates)
      assert(index === 29)
      assert(candidates.toSeq === Seq("db_source", "db_test"))
    }

    "show tables --on dev db_dest db_" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("show tables --on dev db_dest db_", 32, candidates)
      assert(index === 29)
      assert(candidates.toSeq === Seq("db_source", "db_test"))
    }

    "show tables --on dev db_source.source db_" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("show tables --on dev db_source.source db_", 41, candidates)
      assert(index === 38)
      assert(candidates.toSeq === Seq("db_dest", "db_source", "db_test"))
    }

    "show tables --on dev db_source.source db_source." in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("show tables --on dev db_source.source db_source.", 48, candidates)
      assert(index === 48)
      assert(candidates.toSeq === Seq("source_view"))
    }

    "describe tables --on dev --" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("describe tables --on dev --", 27, candidates)
      assert(index === 25)
      assert(candidates.toSeq === Seq("--bytes"))
    }

    "show graph --from " in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("show graph --from ", 18, candidates)
      assert(index === 18)
      assert(candidates.toSeq === Seq("db_dest", "db_source", "db_test"))
    }

    "show graph --from db_d" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("show graph --from db_d", 22, candidates)
      assert(index === 18)
      assert(candidates.toSeq === Seq("db_dest"))
    }

    "show graph --from db_source.source db_source." in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("show graph --from db_source.source db_source.", 45, candidates)
      assert(index === 45)
      assert(candidates.toSeq === Seq("source_view"))
    }

    "describe tables (with ItemName completion)" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("describe tables ", 16, candidates)
      assert(index === 16)
      assert(candidates.toSeq === Seq("db_dest", "db_source", "db_test"))
    }

    "describe tables db_s (with ItemName completion)" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("describe tables db_s", 20, candidates)
      assert(index === 16)
      assert(candidates.toSeq === Seq("db_source"))
    }

    "describe tables db_test. (with ItemName completion)" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("describe tables db_test.", 24, candidates)
      assert(index === 24)
      assert(candidates.toSeq === Seq("test"))
    }

    "describe tables db_test.t (with ItemName completion)" in {
      val candidates = new java.util.ArrayList[CharSequence]()
      val index = shellCompleter.complete("describe tables db_test.t", 25, candidates)
      assert(index === 24)
      assert(candidates.toSeq === Seq("test"))
    }


  }



}
