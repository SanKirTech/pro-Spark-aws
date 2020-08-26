package com.sankir.smp.connectors

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funsuite.AnyFunSuite


class PubSubIOTest extends AnyFlatSpec with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val pubsubIO = PubSubIO("sankir-1705","sample")
    println("Before All")
  }

  override protected def afterAll(): Unit =  {
    super.afterAll()
    println("after all")
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
