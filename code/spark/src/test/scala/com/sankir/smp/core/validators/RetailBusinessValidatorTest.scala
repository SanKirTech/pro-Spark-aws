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
import com.sankir.smp.common.JsonUtils

// Total of 23 tests in this suite
class RetailBusinessValidatorTest extends AnyFlatSpec {

  //  RetailBusinessvalidator - 3 tests
  behavior of "RetailBusinessvalidator"
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
    // All the fields are correct
  }

  it should "return Failure when one of the business rules is invalid in json" in {
    // country name is wrong ( not in the validCountryList)
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
        |    "Country": "xyz"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(RetailBusinessValidator.validate(jsonNode).isFailure)
  }

  it should "return Failure when the business rules are invalid in json " in {
    // Invalid Invoie No, Stockcode, Quantity is -ve, InvoiceDate has month val as 24
    // UnitPrice is -ve, Customer ID is string and country name is wrong ( not in the validCountryList)
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
  }

  //   validStockCode - 4 tests
  behavior of "validStockCode"
  it should "return false when valid StockCode Key is not present in json" in {
    // StockCode key is absent , instead wrong 'Stocked' key is present
    val jsonString =
      """
        |{
        |    "Stockedddd": "abc"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validStockCode().test(jsonNode))
  }

  it should "return false when StockCode value is empty in json" in {
    // StockCode key is present,  but StockCode value is invalid because it is empty
    val jsonString =
      """
        |{
        |    "StockCode": ""
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validStockCode().test(jsonNode))
  }

  it should "return false when StockCode is invalid" in {
    // StockCode key is present,  but StockCode value is empty
    val jsonString =
      """
        |{
        |    "StockCode": "1234Z"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validStockCode().test(jsonNode))
  }

  it should "return true when StockCode is valid" in {
    // StockCode key is present,  and StockCode value is valid
    val jsonString =
      """
        |{
        |    "StockCode": "85123A"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(validStockCode().test(jsonNode))
  }

  //   validCountry - 2 tests
  behavior of "validCountry"
  it should "return false when Country is invalid" in {
    // Country value 'Venezula' is not present in validCountryList
    val jsonString =
      """
        |{
        |    "Country": "Venezula"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validCountry().test(jsonNode))
  }

  it should "return true when Country is valid" in {
    // Country value 'France' is present in validCountryList
    val jsonString =
      """
        |{
        |    "Country": "France"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(validCountry().test(jsonNode))
  }

  //   validInvoices - 2 tests
  behavior of "validInvoices"
  it should "return false when InvoiceNo starts with C" in {
    // InvoiceNo should not start with C
    val jsonString =
      """
        |{
        |    "InvoiceNo": "C84123"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validInvoices().test(jsonNode))
  }

  it should "return true when InvoiceNo is correct" in {
    // valid InvoiceNo present
    val jsonString =
      """
        |{
        |    "InvoiceNo": "536365"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(validInvoices().test(jsonNode))
  }

  //   validQuantity - 3 tests
  behavior of "validQuantity"
  it should "return false when Quantity is -ve" in {
    // Quantity should not be -ve
    val jsonString =
      """
        |{
        |    "Quantity": -1
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validQuantity().test(jsonNode))
  }

  it should "return false when Quantity is Zero" in {
    // Quantity should not be zero
    val jsonString =
      """
        |{
        |    "Quantity": 0
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validQuantity().test(jsonNode))
  }

  it should "return true when Quantity is +ve" in {
    // valid Quantity present
    val jsonString =
      """
        |{
        |    "Quantity": 6
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(validQuantity().test(jsonNode))
  }

  // validUnitPrice - 3 tests
  behavior of "validUnitPrice"
  it should "return false when UnitPrice is -ve" in {
    // UnitPrice should not be -ve
    val jsonString =
      """
        |{
        |    "UnitPrice": -1
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validUnitPrice().test(jsonNode))
  }

  it should "return false when UnitPrice is Zero" in {
    // UnitPrice should not be Zero
    val jsonString =
      """
        |{
        |    "UnitPrice": 0
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validUnitPrice().test(jsonNode))
  }

  it should "return true when UnitPrice is +ve" in {
    // valid UnitPrice present
    val jsonString =
      """
        |{
        |    "UnitPrice": 2.55
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(validUnitPrice().test(jsonNode))
  }

  // validCustomerID - 2 tests
  behavior of "validCustomerID"
  it should "return false when CustomerID is empty" in {
    // CustomerID is empty
    val jsonString =
      """
        |{
        |    "CustomerID": ""
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validCustomerId().test(jsonNode))
  }

  it should "return true when CustomerID is correct" in {
    // valid CustomerID present
    val jsonString =
      """
        |{
        |    "CustomerID": 17850.0
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(validCustomerId().test(jsonNode))
  }

  //  validInvoiceDate - 4 tests
  behavior of "validInvoiceDate"
  it should "return true when InvoiceDate is correct" in {
    // valid InvoiceDate present
    val jsonString =
      """
        |{
        |    "InvoiceDate": "2010-12-01 08:26:00"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(validDate().test(jsonNode))
  }


  it should "return false when InvoiceDate is having date > today date" in {
    // InvoiceDate is > today's date, 2023-12-01
    val jsonString =
      """
        |{
        |    "InvoiceDate": "2023-12-01 08:26:00"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validDate().test(jsonNode))
  }

  it should "return false when InvoiceDate is having wrong month" in {
    // InvoiceDate is having wrong month value of 40
    val jsonString =
      """
        |{
        |    "InvoiceDate": "2010-40-01 08:26:00"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validDate().test(jsonNode))
  }

  it should "return false when InvoiceDate is having wrong hour" in {
    // InvoiceDate is having wrong hour value of 86
    val jsonString =
      """
        |{
        |    "InvoiceDate": "2010-10-01 08:86:00"
        |}
        |""".stripMargin
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(!validDate().test(jsonNode))
  }

}
