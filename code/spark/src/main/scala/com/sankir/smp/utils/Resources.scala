package com.sankir.smp.utils

import java.io.InputStream


object Resources {
  def read(path: String) : InputStream =
    Resources.getClass.getClassLoader.getResourceAsStream(path)

  def readAsString(path: String) : String = {
    scala.io.Source.fromInputStream(read(path)).getLines().mkString("\n")
  }

}
