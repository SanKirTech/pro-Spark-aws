/*
 *
 *  * SanKir Technologies
 *  * (c) Copyright 2021.  All rights reserved.
 *  * No part of pro-Spark course contents - code, video or documentation - may be reproduced, distributed or transmitted
 *  *  in any form or by any means including photocopying, recording or other electronic or mechanical methods,
 *  *  without the prior written permission from Sankir Technologies.
 *  *
 *  * The course contents can be accessed by subscribing to pro-Spark course.
 *  *
 *  * Please visit www.sankir.com for details.
 *  *
 *
 */

package com.sankir.smp.core.validators

import org.scalatest.flatspec.AnyFlatSpec
import com.sankir.smp.core.validators.RetailBusinessValidator._
import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.common.JsonUtils
import org.scalatest._
import matchers.should.Matchers._


class RetailBusinessValidatorTest extends AnyFlatSpec {

  behavior of "RetailBusiness validator"
  it should "return success when all the business rules are valid in json" in {
    val jsonString =
      """
        |{
        |    "InvoiceNo": "536365",
        |    "StockCode": "85123A",
        |    "Description": "T-LIGHT HOLDER",
        |    "Quantity": 6,
        |    "InvoiceDate": "2010-12-01 08:26:00",
        |    "UnitPrice": 2.55,
        |    "CustomerID": 17850.0,
        |    "Country": "United Kingdom"
        |}
        |""".stripMargin

    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(RetailBusinessValidator.validate(jsonNode).isSuccess)
    //RetailBusinessValidator.validate(jsonNode).isSuccess shouldBe true

    // All the fields are correct

  }

  it should "return Failure when one of the business rules is invalid in json" in {
    val jsonString =
      """
        |{
        |    "InvoiceNo": "Failed-invoice-number",
        |    "StockCode": "85123A",
        |    "Description": "T-LIGHT HOLDER",
        |    "Quantity": 6,
        |    "InvoiceDate": "2010-12-01 08:26:00",
        |    "UnitPrice": 2.55,
        |    "CustomerID": 17850.0,
        |    "Country": "xyz"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(RetailBusinessValidator.validate(jsonNode).isFailure)

    // country name is wrong ( not in the validCountryList)
  }

  it should "return Failure when the business rules are invalid in json " in {
    val jsonString =
      """
        |{
        |    "InvoiceNo": "Wrong-invoice-number",
        |    "StockCode": "Wrong-StockCode",
        |    "Description": "T-LIGHT HOLDER",
        |    "Quantity": -100,
        |    "InvoiceDate": "2010-24-01 08:26:00",
        |    "UnitPrice": -2.5,
        |    "CustomerID": "abc",
        |    "Country": "xyz"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(RetailBusinessValidator.validate(jsonNode).isFailure)

    // Invalid Invoie No, Stockcode, Quantity is -ve, InvoiceDate has month val as 24
    // UnitPrice is -ve, Customer ID is string and country name is wrong ( not in the validCountryList)
  }

  //   behavior of validStockCode

  behavior of "validStockCode"
  it should "return false when valid StockCode Key is not present in json" in {
    val jsonString =
      """
        |{
        |    "Stocked": "abc"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validStockCode().test(jsonNode))

    // StockCode key is absent , insted wrong 'Stocked' key is present
  }

  it should "return false when StockCode value is empty in json" in {
    val jsonString =
      """
        |{
        |    "StockCode": ""
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validStockCode().test(jsonNode))

    // StockCode key is present,  but StockCode value is invalid because it is empty
  }

  it should "return false when StockCode is invalid" in {
    val jsonString =
      """
        |{
        |    "StockCode": "1234Z"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validStockCode().test(jsonNode))

    // StockCode key is present,  but StockCode value is empty
  }

  it should "return true when StockCode is valid" in {
    val jsonString =
      """
        |{
        |    "StockCode": "85123A"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(validStockCode().test(jsonNode))

    // StockCode key is present,  and StockCode value is valid
  }

  //   behavior of validCountry

  behavior of "validCountry"
  it should "return false when Country is invalid" in {
    val jsonString =
      """
        |{
        |    "Country": "Venezula"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validCountry().test(jsonNode))

    // Country value 'Venezula' is not present in validCountryList
  }

  it should "return true when Country is valid" in {
    val jsonString =
      """
        |{
        |    "Country": "France"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(validCountry().test(jsonNode))

    // Country value 'France' is present in validCountryList
  }

  //   behavior of validInvoices

  behavior of "validInvoices"
  it should "return false when InvoiceNo starts with C" in {
    val jsonString =
      """
        |{
        |    "InvoiceNo": "C84123"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validInvoices().test(jsonNode))

    // InvoiceNo should not start with C
  }

  it should "return true when InvoiceNo is correct" in {
    val jsonString =
      """
        |{
        |    "InvoiceNo": "536365"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(validInvoices().test(jsonNode))

    // valid InvoiceNo present

  }

  //   behavior of validQuantity

  behavior of "validQuantity"
  it should "return false when Quantity is -ve" in {
    val jsonString =
      """
        |{
        |    "Quantity": -1
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validQuantity().test(jsonNode))

    // Quantity should not be -ve
  }

  it should "return false when Quantity is Zero" in {
    val jsonString =
      """
        |{
        |    "Quantity": 0
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validQuantity().test(jsonNode))

    // Quantity should not be zero
  }

  it should "return true when Quantity is +ve" in {
    val jsonString =
      """
        |{
        |    "Quantity": 6
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(validQuantity().test(jsonNode))

    // valid Quantity present
  }

  // behavior of validUnitPrice

  behavior of "validUnitPrice"
  it should "return false when UnitPrice is -ve" in {
    val jsonString =
      """
        |{
        |    "UnitPrice": -1
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validUnitPrice().test(jsonNode))

    // UnitPrice should not be -ve
  }

  it should "return false when UnitPrice is Zero" in {
    val jsonString =
      """
        |{
        |    "UnitPrice": 0
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validUnitPrice().test(jsonNode))

    // UnitPrice should not be Zero
  }

  it should "return true when UnitPrice is +ve" in {
    val jsonString =
      """
        |{
        |    "UnitPrice": 2.55
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(validUnitPrice().test(jsonNode))
    // valid UnitPrice present
  }

  // behavior of "validCustomerID"

  behavior of "validCustomerID"
  it should "return false when CustomerID is empty" in {
    val jsonString =
      """
        |{
        |    "CustomerID": ""
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validCustomerId().test(jsonNode))

    // CustomerID is empty
  }

  it should "return true when CustomerID is correct" in {
    val jsonString =
      """
        |{
        |    "CustomerID": 17850.0
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(validCustomerId().test(jsonNode))

    // valid CustomerID present
  }

  //  behavior of validDate

  behavior of "validInvoiceDate"
  it should "return true when InvoiceDate is correct" in {
    val jsonString =
      """
        |{
        |    "InvoiceDate": "2010-12-01 08:26:00"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(validDate().test(jsonNode))

    // valid InvoiceDate present
  }


  it should "return false when InvoiceDate is having date > today date" in {
    val jsonString =
      """
        |{
        |    "InvoiceDate": "2023-12-01 08:26:00"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validDate().test(jsonNode))

    // InvoiceDate is > today's date, 2023-12-01
  }

  it should "return false when InvoiceDate is having wrong month" in {
    val jsonString =
      """
        |{
        |    "InvoiceDate": "2010-40-01 08:26:00"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validDate().test(jsonNode))

    // InvoiceDate is having wrong month value of 40
  }

  it should "return false when InvoiceDate is having wrong hour" in {
    val jsonString =
      """
        |{
        |    "InvoiceDate": "2010-40-01 08:86:00"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validDate().test(jsonNode))

    // InvoiceDate is having wrong hour value of 86
  }

}
