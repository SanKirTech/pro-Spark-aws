package com.sankir.smp.pipelines.retail

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.common.validators.{Matcher, Matchers}
import com.sankir.smp.pipelines.validators.BusinessValidator
import com.sankir.smp.utils.Resources
import com.sankir.smp.utils.exceptions.BusinessValidationFailedException

import scala.util.{Failure, Success, Try}

object RetailBusinessValidator extends BusinessValidator{
  override def validate(data: JsonNode): Try[JsonNode] = {
    if (Matchers.and(List(
      validStockCode(),
      validCountry(),
      validInvoices(),
      validQuantity(),
      validUnitPrice(),
      validCustomerId(),
      validDate()
    )).test(data))
      Success(data)
      else Failure(BusinessValidationFailedException("Business Validation for retail failed"))
  }


  def validStockCode(): Matcher[JsonNode] = {
    val validStockCodes = Resources.readAsStringIterator("validStockCode.txt").toSet
    new Matcher[JsonNode] {
      override def test(t: JsonNode): Boolean = Try(validStockCodes.contains(t.get("StockCode").asText())).getOrElse(false)
    }
  }

  def validCountry(): Matcher[JsonNode] = {
    val validCountries = Resources.readAsStringIterator("validCountryList.txt").toSet
    new Matcher[JsonNode] {
      override def test(t: JsonNode): Boolean = Try(validCountries.contains(t.get("Country").asText())).getOrElse(false)
    }
  }

  def validInvoices(): Matcher[JsonNode] = {
    new Matcher[JsonNode] {
//      override def test(t: JsonNode): Boolean = Try(!(t.get("InvoiceNo").asText().startsWith("C") | t.get("InvoiceNo").asText().startsWith("I")) ).getOrElse(false)
      override def test(t: JsonNode): Boolean = Try(!t.get("InvoiceNo").asText().startsWith("C")).getOrElse(false)
    }
  }

  def validQuantity(): Matcher[JsonNode] = {
    new Matcher[JsonNode] {
      override def test(t: JsonNode): Boolean = Try( t.get("Quantity").asLong() > 0).getOrElse(false)
    }
    // its treated as String, so it wil work even if valid is given in double quotes ,
    // hence using asLong typecast
    // sincewe give vlaue directly as LOng without quotes in json file, it still works for typecast of Long asLong !
  }

  def validUnitPrice(): Matcher[JsonNode] = {
    new Matcher[JsonNode] {
      override def test(t: JsonNode): Boolean = Try( t.get("UnitPrice").asDouble() > 0).getOrElse(false)
    }

    /*
    override def test(t: JsonNode): Boolean = Try( t.get("UnitPrice").asDouble() > 0).getOrElse(false)

    t.get("UnitPrice").asDouble() > 0   -> is true for value of 10
    getorElse returns true  ( whichever above line returns )
    otherwise ( if valueis -10 )
    getorElse returns default  ( whichever is set here it is set to false  )

     */
  }

  def validCustomerId(): Matcher[JsonNode] = {
    new Matcher[JsonNode] {
      override def test(t: JsonNode): Boolean = Try(!t.get("CustomerID").asText().isEmpty).getOrElse(false)
    }
  }

  def validDate(): Matcher[JsonNode] = {
    new Matcher[JsonNode] {
      val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
//      override //def test(t: JsonNode): Boolean = Try(!LocalDateTime.from(dateTimeFormatter.parse(t.get("InvoiceDate").asText())).getDayOfWeek().toString.isEmpty).getOrElse(false)
      override def test(t: JsonNode): Boolean = Try(!LocalDateTime.from(dateTimeFormatter.parse(t.get("InvoiceDate").asText())).isAfter(LocalDateTime.now())).getOrElse(false)
    }
  }

}
