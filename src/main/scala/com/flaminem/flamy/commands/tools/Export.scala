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

package com.flaminem.flamy.commands.tools

import com.flaminem.flamy.commands.utils.FlamySubcommand
import com.flaminem.flamy.conf.{Environment, FlamyContext, FlamyContextFormatter, FlamyGlobalOptions}
import com.flaminem.flamy.exec.FlamyRunner
import com.flaminem.flamy.exec.utils.{ReturnFailure, ReturnStatus, ReturnSuccess}
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model.core.Model
import com.flaminem.flamy.model.exceptions.UnexpectedBehaviorException
import com.flaminem.flamy.model.names.ItemName
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

/**
  * Created by fpin on 7/4/17.
  */
class Export extends Subcommand("export") with FlamySubcommand {

  val conf = new Subcommand("conf") with FlamySubcommand {
    banner("Automatically generate a configuration template or doc")
    private lazy val template: ScallopOption[Boolean] = toggle(name = "template", default = Some(false), noshort = true)
    private lazy val markdown: ScallopOption[Boolean] = toggle(name = "markdown", default = Some(false), noshort = true)
    private lazy val rst: ScallopOption[Boolean] = toggle(name = "rst", default = Some(false), noshort = true)

    requireOne(template, markdown, rst)

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val context = new FlamyContext(globalOptions, Some(Environment("<ENV>")))
      if(template()) {
        FlamyOutput.out.println(new FlamyContextFormatter(context).toTemplate)
      }
      else if(markdown()) {
        FlamyOutput.out.println(new FlamyContextFormatter(context).toMarkdown)
      }
      else if(rst()) {
        FlamyOutput.out.println(new FlamyContextFormatter(context).toRST)
      }
      else {
        throw new UnexpectedBehaviorException("Either --template or --markdown option should be used")
      }
      ReturnSuccess
    }
  }

  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
    subCommands match {
      case  (command: FlamySubcommand)::Nil => command.doCommand(globalOptions, Nil)
      case Nil => throw new IllegalArgumentException("A subcommand is expected")
      case _ =>
        printHelp()
        ReturnFailure
    }
  }

}
