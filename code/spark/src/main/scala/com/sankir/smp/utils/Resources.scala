/*
 * SanKir Technologies
 * (c) Copyright 2020.  All rights reserved.
 * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  without the prior written permission from Sankir Technologies.
 *
 * The course contents can be accessed by subscribing to pro-Spark course.
 *
 * Please visit www.sankir.com for details.
 *
 */

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
