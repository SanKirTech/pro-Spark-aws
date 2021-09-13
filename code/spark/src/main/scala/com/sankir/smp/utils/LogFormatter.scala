package com.sankir.smp.utils

object LogFormatter {

  def formatLogger(msg: String,
                   prefix: String = "",
                   suffix: String = ""): String = {
    val msgBuilder = new StringBuilder();
    msgBuilder.append("\n")
    msgBuilder.append(prefix * 100)
    msgBuilder.append(msg)
    msgBuilder.append(suffix * 100)
    msgBuilder.append("\n")
    msgBuilder.toString()
  }

  def formatHeader(msg: String,
                   prefix: String = "-",
                   suffix: String = "-"): String = {
    val msgBuilder = new StringBuilder();
    msgBuilder.append("\n")
    msgBuilder.append(prefix * 30)
    msgBuilder.append(" ")
    msgBuilder.append(msg)
    msgBuilder.append(" ")
    msgBuilder.append(suffix * 30)
    msgBuilder.append("\n")
    msgBuilder.toString()
  }

}
