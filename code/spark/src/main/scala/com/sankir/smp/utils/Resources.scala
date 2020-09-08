package com.sankir.smp.utils

import java.io.InputStream

import com.sankir.smp.connectors.GcsIO


object Resources {
  def readAsStringIterator(path: String): Iterator[String] = {
    scala.io.Source.fromInputStream(read(path)).getLines()
  }

  def readAsString(path: String): String = {
    readAsStringIterator(path).mkString("\n")
  }

  def read(path: String): InputStream =
    Resources.getClass.getClassLoader.getResourceAsStream(path)

  def readAsStringFromGCS(projectId: String, path: String): String = {
    val gcsIO = GcsIO(projectId = projectId)
    gcsIO.getData(path)
  }

}
