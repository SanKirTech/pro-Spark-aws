package com.sankir.smp.core

import com.sankir.smp.core.CloudInitializer._

object ApplicationInitializer {
  def main(args: Array[String]): Unit = {
    initializeCloud()
    AppMain.run(args, cloudConnector, cloudConfig);
  }
}


