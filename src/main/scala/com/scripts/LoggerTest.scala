package com.scripts
import com.config.KipLogger

object LoggerTest extends KipLogger{
  def main(args: Array[String]) = {

    info("Info")
    warn("error")
  }


}
