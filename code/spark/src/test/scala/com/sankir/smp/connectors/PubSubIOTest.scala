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

package com.sankir.smp.connectors

import java.io.{File, FileInputStream}

import com.google.auth.oauth2.ServiceAccountCredentials
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite

@Ignore
class PubSubIOTest extends AnyFlatSpec with BeforeAndAfterAll {

  var pubSubIO: PubSubIO = null;

  override def beforeAll(): Unit = {
    super.beforeAll()
    val credentialsPath = new File("F:\\extra-work\\lockdown_usecases\\SparkUsecase\\key.json")
    val googleCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(credentialsPath))
    pubSubIO = PubSubIO("sankir-1705","sample")
  }

  override protected def afterAll(): Unit =  {
    super.afterAll()
    pubSubIO.close()
  }

  behavior of "pubsubio"

  it should "send message to pubsub" in {
      assert(pubSubIO.publishMessage("message") != null)


  }

//  test("pubsubIO test") {
//    val pubsubIO = PubSubIO("sankir-1705","sample")
//    pubsubIO.publishMessage("testMessage")
////    pubsubIO.close()
//    assert(1 == pubsubIO.publishMessage("testMessage"))
//
//  }
//  test("pubsubIO test 2") {
//    assert(1 == 1)
//  }
//
//  test("pubsubIO test 3") {
//    assert(1 == 4)
//  }

}
