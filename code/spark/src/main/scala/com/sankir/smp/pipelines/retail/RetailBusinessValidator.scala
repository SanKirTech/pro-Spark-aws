package com.sankir.smp.pipelines.retail

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.common.validators.{Matcher, Matchers}
import com.sankir.smp.pipelines.validators.BusinessValidator
import com.sankir.smp.utils.Resources
import com.sankir.smp.utils.exceptions.BusinessValidationFailedException

import scala.collection.mutable
import scala.collection.parallel.immutable
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
      override def test(t: JsonNode): Boolean = Try(t.get("InvoiceNo").asText().startsWith("C")).getOrElse(false)
    }
  }

  def validQuantity(): Matcher[JsonNode] = {
    new Matcher[JsonNode] {
      override def test(t: JsonNode): Boolean = Try( t.get("Quantity").asLong() > 0).getOrElse(false)
    }
  }

  def validUnitPrice(): Matcher[JsonNode] = {
    new Matcher[JsonNode] {
      override def test(t: JsonNode): Boolean = Try( t.get("UnitPrice").asDouble() > 0).getOrElse(false)
    }
  }

  def validCustomerId(): Matcher[JsonNode] = {
    new Matcher[JsonNode] {
      override def test(t: JsonNode): Boolean = Try(!t.get("CustomerID").asText().isEmpty).getOrElse(false)
    }
  }

  def validDate(): Matcher[JsonNode] = {
    new Matcher[JsonNode] {
      val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      override def test(t: JsonNode): Boolean = Try(!LocalDateTime.from(dateTimeFormatter.parse(t.get("InvoiceDate").asText())).getDayOfWeek().toString.isEmpty).getOrElse(false)
    }
  }

}
