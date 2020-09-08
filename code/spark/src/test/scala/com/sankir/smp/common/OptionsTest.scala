package com.sankir.smp.common

import org.scalatest.flatspec.AnyFlatSpec

class OptionsTest extends AnyFlatSpec {

  behavior of "or"

  it should "return the 1st option when 2nd is empty" in {
    val result = Options.or(Option("1"),Option.empty)
    assert(result.contains("1"))
  }

  it should "return the 2nd option when 1st is empty" in {
    val result = Options.or(Option.empty,Option("2"))
    assert(result.contains("2"))
  }

  it should "return the 1st option when both are present" in {
    val result = Options.or(Option("1"),Option("2"))
    assert(result.contains("1"))
  }

  it should "return empty when both are empty" in {
    val result = Options.or(Option.empty, Option.empty)
    assert(result.isEmpty)
  }

  behavior of "productK"

  it should "apply function if both values are present" in {
    val result = Options.productK(Option(1), Option(2), (a : Int,b: Int) => Option(a+b))
    assert(result.contains(3))
  }

  it should "return empty if one is empty" in {
    val result = Options.productK(Option(1), Option.empty, (a : Int,b: Int) => Option(a+b))
    assert(result.isEmpty)
  }

  it should "return empty if both are empty" in {
    val result = Options.productK(Option.empty, Option.empty, (a : Int,b: Int) => Option(a+b))
    assert(result.isEmpty)
  }
}
