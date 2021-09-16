package com.sankir.smp.core

import com.sankir.smp.core.CloudInitializer._

/**
 * This is the main Entry point of the program
 * <p>It will initialize the cloud setup and will pass the configurations to `ProSparkApp`
 */
object ApplicationMain {
  def main(args: Array[String]): Unit = {
    initializeCloud()
    ProSparkApp.run(args, cloudConnector, cloudConfig)
  }
}
