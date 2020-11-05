package com.sankir.smp.core.retail

import org.scalatest.flatspec.AnyFlatSpec
import com.sankir.smp.core.validators.RetailBusinessValidator._
import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.common.JsonUtils
import com.sankir.smp.core.validators.RetailBusinessValidator

class RetailBusinessDataValidatorTest extends AnyFlatSpec {

  behavior of "RetailBusiness validator"
  it should "return success when all the business rules are met" in {
    val jsonString = "{\"InvoiceNo\": \"C536365\", \"StockCode\": \"85123A\", \"Description\": \"WHITE HANGING HEART T-LIGHT HOLDER\", \"Quantity\": \"6\", \"InvoiceDate\": \"2010-12-01 08:26:00\", \"UnitPrice\": \"2.55\", \"CustomerID\": \"17850.0\", \"Country\": \"United Kingdom\"}"
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(RetailBusinessValidator.validate(jsonNode).isSuccess)
  }

  it should "return Failure when one of the business rules fails" in {
    val jsonString = "{\"InvoiceNo\": \"Failed-invoice-number\", \"StockCode\": \"85123A\", \"Description\": \"WHITE HANGING HEART T-LIGHT HOLDER\", \"Quantity\": \"6\", \"InvoiceDate\": \"2010-12-01 08:26:00\", \"UnitPrice\": \"2.55\", \"CustomerID\": \"17850.0\", \"Country\": \"United Kingdom\"}"
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(RetailBusinessValidator.validate(jsonNode).isFailure)
  }

  it should "return Failure when all the business rules are failed" in {
    val jsonString = "{\"InvoiceNo\": \"Failed-invoice-number\", \"StockCode\": \"Failed-StockCode\", \"Description\": \"WHITE HANGING HEART T-LIGHT HOLDER\", \"Quantity\": \"0\", \"InvoiceDate\": \"2010-12-008:26:00\", \"UnitPrice\": \"abc\", \"CustomerID\": \"\", \"Country\": \"United\"}"
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(RetailBusinessValidator.validate(jsonNode).isFailure)
  }

  it should "return Failure when wrong json is passed" in {
    val jsonString = "{\"key\":\"value\"}"
    val jsonNode = JsonUtils.toJsonNode(jsonString)
    assert(RetailBusinessValidator.validate(jsonNode).isFailure)
  }

//  behavior of  "validStockCode"
//  it should "return false when Empty Json are passed" in {
//    assert(!validStockCode().test(JsonUtils.emptyObject().asInstanceOf[JsonNode]))
//  }
//  it should "return false when StockCode Key is not present in json" in {
//    assert(!validStockCode().test(JsonUtils.emptyObject().put("Quantity", "abc").asInstanceOf[JsonNode]))
//  }
//  it should "return false when StockCode not present in validjsonList" in {
//    assert(!validStockCode().test(JsonUtils.emptyObject().put("StockCode", "not-present").asInstanceOf[JsonNode]))
//  }
//  it should "return true when Success StockCode present in validjsonList" in {
//    assert(validStockCode().test(JsonUtils.emptyObject().put("StockCode", "abc").asInstanceOf[JsonNode]))
//  }

  behavior of  "validCountry"
  it should "return false when Empty Json are passed" in {
    assert(!validCountry().test(JsonUtils.emptyObject().asInstanceOf[JsonNode]))
  }
  it should "return false when Country Key is not present in json" in {
    assert(!validCountry().test(JsonUtils.emptyObject().put("XYZ", "abc").asInstanceOf[JsonNode]))
  }
  it should "return false when Country is empty" in {
    assert(!validCountry().test(JsonUtils.emptyObject().put("Country", "").asInstanceOf[JsonNode]))
  }
  it should "return false when Country not present in CountryList" in {
    assert(!validCountry().test(JsonUtils.emptyObject().put("Country", "Venezula").asInstanceOf[JsonNode]))
  }
  it should "return true when Country present in CountryList" in {
    // Country present in json
    assert(validCountry().test(JsonUtils.emptyObject().put("Country", "France").asInstanceOf[JsonNode]))
  }

  behavior of  "validInvoices"
  it should "return false when Empty Json is passed" in {
    assert(!validInvoices().test(JsonUtils.emptyObject().asInstanceOf[JsonNode]))
  }
  it should "return false when InvoiceNo Key is not present in json" in {
    assert(!validInvoices().test(JsonUtils.emptyObject().put("XYZ", "abc").asInstanceOf[JsonNode]))
  }
  it should "return false when InvoiceNo is Empty" in {
    assert(!validInvoices().test(JsonUtils.emptyObject().put("InvoiceNo", " ").asInstanceOf[JsonNode]))
  }
  it should "return false when InvoiceNo starts with C" in {
    assert(!validInvoices().test(JsonUtils.emptyObject().put("InvoiceNo", "C12345").asInstanceOf[JsonNode]))
  }
  it should "return true when InvoiceNo starts with C" in {
    assert(validInvoices().test(JsonUtils.emptyObject().put("InvoiceNo", "541267").asInstanceOf[JsonNode]))
  }

  behavior of  "validQuantity"
  it should "return false when Empty Json is passed" in {
    assert(!validQuantity().test(JsonUtils.emptyObject().asInstanceOf[JsonNode]))
  }
  it should "return false when Quantity Key is not present in json" in {
    assert(!validQuantity().test(JsonUtils.emptyObject().put("XYZ", "abc").asInstanceOf[JsonNode]))
  }
  it should "return false when Quantity is Empty" in {
    assert(!validQuantity().test(JsonUtils.emptyObject().put("Quantity", "").asInstanceOf[JsonNode]))
  }
  it should "return false when Quantity is -ve" in {
    assert(!validQuantity().test(JsonUtils.emptyObject().put("Quantity", -1).asInstanceOf[JsonNode]))
  }
  it should "return false when Quantity is 0" in {
    assert(!validQuantity().test(JsonUtils.emptyObject().put("Quantity", 0).asInstanceOf[JsonNode]))
  }
  it should "return true when Quantity is positive" in {
    assert(validQuantity().test(JsonUtils.emptyObject().put("Quantity", 10).asInstanceOf[JsonNode]))
  }

  behavior of  "validUnitPrice"
  it should "return false when Empty Json is passed" in {
    assert(!validUnitPrice().test(JsonUtils.emptyObject().asInstanceOf[JsonNode]))
  }
  it should "return false when UnitPrice Key is not present in json" in {
    assert(!validUnitPrice().test(JsonUtils.emptyObject().put("XYZ", "abc").asInstanceOf[JsonNode]))
  }
  it should "return false when UnitPrice is Empty" in {
    assert(!validUnitPrice().test(JsonUtils.emptyObject().put("UnitPrice", "").asInstanceOf[JsonNode]))
  }
  it should "return false when UnitPrice is -ve" in {
    assert(!validUnitPrice().test(JsonUtils.emptyObject().put("UnitPrice", "-1.0").asInstanceOf[JsonNode]))
  }
  it should "return false when UnitPrice is 0" in {
    assert(!validUnitPrice().test(JsonUtils.emptyObject().put("UnitPrice", "0.0").asInstanceOf[JsonNode]))
  }
  it should "return true when UnitPrice is positive" in {
    assert(validUnitPrice().test(JsonUtils.emptyObject().put("UnitPrice", "10.50").asInstanceOf[JsonNode]))
  }

  behavior of  "validCustomerID"
  it should "return false when Empty Json is passed" in {
    assert(!validCustomerId().test(JsonUtils.emptyObject().asInstanceOf[JsonNode]))
  }
  it should "return false when CustomerID Key is not present in json" in {
    assert(!validCustomerId().test(JsonUtils.emptyObject().put("XYZ", "abc").asInstanceOf[JsonNode]))
  }
  it should "return false when CustomerID is Empty" in {
    assert(!validCustomerId().test(JsonUtils.emptyObject().put("CustomerID", "").asInstanceOf[JsonNode]))
  }
  it should "return true when CustomerID is correct" in {
    assert(validCustomerId().test(JsonUtils.emptyObject().put("CustomerID", 12779.0).asInstanceOf[JsonNode]))
  }

  behavior of  "validDate"
  it should "return false when Empty Json is passed" in {
    assert(!validDate().test(JsonUtils.emptyObject().asInstanceOf[JsonNode]))
  }
  it should "return false when InvoiceDate is not present in json" in {
    assert(!validDate().test(JsonUtils.emptyObject().put("XYZ", "abc").asInstanceOf[JsonNode]))
  }
  it should "return false when InvoiceDate is Empty" in {
    assert(!validDate().test(JsonUtils.emptyObject().put("InvoiceDate", "").asInstanceOf[JsonNode]))
  }
  it should "return false when InvoiceDate is not date" in {
    assert(!validDate().test(JsonUtils.emptyObject().put("InvoiceDate", "not-present").asInstanceOf[JsonNode]))
  }
  it should "return true when InvoiceDate is proper" in {
    assert(validDate().test(JsonUtils.emptyObject().put("InvoiceDate", "2010-12-01 08:26:00").asInstanceOf[JsonNode]))
  }
  it should "return false when InvoiceDate is having wrong day " in {
    assert(!validDate().test(JsonUtils.emptyObject().put("InvoiceDate", "2011-01-51 08:26:00").asInstanceOf[JsonNode]))
  }
  it should "return false when InvoiceDate is having wrong month " in {
    assert(!validDate().test(JsonUtils.emptyObject().put("InvoiceDate", "2011-41-01 08:26:00").asInstanceOf[JsonNode]))
  }
  it should "return false when InvoiceDate is having date > today date  " in {
    assert(!validDate().test(JsonUtils.emptyObject().put("InvoiceDate", "2019-01-17 08:26:00").asInstanceOf[JsonNode]))
  }
  it should "return false when InvoiceDate is wrong sec" in {
    assert(!validDate().test(JsonUtils.emptyObject().put("InvoiceDate", "2010-12-01 08:26:70").asInstanceOf[JsonNode]))
  }
  it should "return false when InvoiceDate is wrong min" in {
    assert(!validDate().test(JsonUtils.emptyObject().put("InvoiceDate", "2010-12-01 08:80:00").asInstanceOf[JsonNode]))
  }
  it should "return false when InvoiceDate is wrong hour" in {
    assert(!validDate().test(JsonUtils.emptyObject().put("InvoiceDate", "2010-12-01 90:26:00").asInstanceOf[JsonNode]))
  }
}
