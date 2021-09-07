package com.sankir.smp.utils

import java.io.InputStream

object Resources {
  def readAsStringIterator(path: String): Iterator[String] = {
    scala.io.Source.fromInputStream(read(path)).getLines()
  }

  def readAsString(path: String): String = {
    readAsStringIterator(path).mkString("\n")
  }

  def read(path: String): InputStream =
    Resources.getClass.getClassLoader.getResourceAsStream(path)

  /***
  *
  * @param projectId - Pass the projectid which you have created in GCP
  * @param path  - Path of the schema json which is used as reference to validate the schema
  * @return return the content of the file as string
  */
}
