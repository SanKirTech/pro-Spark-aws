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

/*
 * Comment for pipeline module
 * Scala objects
 * Spark sql table
 * KPI tables
 *
 */

package com.sankir.smp.core.validators

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.fasterxml.jackson.databind.JsonNode
import com.sankir.smp.common.{Matcher, Matchers}
import com.sankir.smp.utils.Resources
import com.sankir.smp.utils.exceptions.BusinessValidationFailedException

import scala.util.{Failure, Success, Try}

object RetailBusinessValidator extends BusinessValidator {

  /***
    *
    * @param data JsonNode object which wraps the json string
    * @return - Success[JsonNode] or Failure message
    */
  def validate(data: JsonNode): Try[JsonNode] = {
    if (Matchers
          .and(
            List(
              validStockCode,
              validCountry(),
              validInvoices(),
              validQuantity(),
              validUnitPrice(),
              validCustomerId(),
              validDate()
            )
          )
          .test(data))
      Success(data)
    else
      Failure(
        BusinessValidationFailedException(
          "Business Validation for retail failed"
        )
      )
  }

  def validStockCode(): Matcher[JsonNode] = {
    val validStockCodes =
      Resources.readAsStringIterator("validStockCode.txt").toSet
    new Matcher[JsonNode] {
      override def test(t: JsonNode): Boolean =
        Try(validStockCodes.contains(t.get("StockCode").asText()))
          .getOrElse(false)
    }
  }

  def validCountry(): Matcher[JsonNode] = {
    val validCountries =
      Resources.readAsStringIterator("validCountryList.txt").toSet
    new Matcher[JsonNode] {
      override def test(t: JsonNode): Boolean =
        Try(validCountries.contains(t.get("Country").asText())).getOrElse(false)
    }
  }

  def validInvoices(): Matcher[JsonNode] = {
    new Matcher[JsonNode] {
      override def test(t: JsonNode): Boolean =
        Try({
          val invoice = t.get("InvoiceNo").asText();
          !(invoice.isEmpty || invoice.startsWith("C"))
        }).getOrElse(false)
    }
  }

  def validQuantity(): Matcher[JsonNode] = {
    new Matcher[JsonNode] {
      override def test(t: JsonNode): Boolean =
        Try(t.get("Quantity").asLong() > 0).getOrElse(false)
    }
  }

  def validUnitPrice(): Matcher[JsonNode] = {
    new Matcher[JsonNode] {
      override def test(t: JsonNode): Boolean =
        Try(t.get("UnitPrice").asDouble() > 0).getOrElse(false)
    }
  }

  def validCustomerId(): Matcher[JsonNode] = {
    new Matcher[JsonNode] {
      override def test(t: JsonNode): Boolean =
        Try(!t.get("CustomerID").asText().isEmpty).getOrElse(false)
    }
  }

  def validDate(): Matcher[JsonNode] = {
    new Matcher[JsonNode] {
      val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      override def test(t: JsonNode): Boolean =
        Try(
          !LocalDateTime
            .from(dateTimeFormatter.parse(t.get("InvoiceDate").asText()))
            .isAfter(LocalDateTime.now())
        ).getOrElse(false)
    }
  }

}
