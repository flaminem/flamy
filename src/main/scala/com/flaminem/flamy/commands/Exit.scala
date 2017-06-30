package com.flaminem.flamy.commands

import com.flaminem.flamy.commands.utils.FlamySubcommand
import com.flaminem.flamy.conf.FlamyGlobalOptions
import com.flaminem.flamy.exec.utils.{ReturnFailure, ReturnStatus}
import org.rogach.scallop.{ScallopConf, Subcommand}

/**
  * A command to exit the shell
  */
class Exit extends Subcommand("exit") with FlamySubcommand{

  banner("Exit this shell")

  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
    ReturnFailure
  }

}

