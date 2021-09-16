package com.sankir.smp.cloud.common.vos

/**
 *
 * @param InvoiceNo - invoie number
 * @param StockCode  - stock code
 * @param Description  - description of the SKU
 * @param Quantity  - Quantity
 * @param InvoiceDate - Date of invoice
 * @param UnitPrice  - unit price of SKU
 * @param CustomerID  - Customer id
 * @param Country  - Country
 */
case class RetailCase(InvoiceNo: String,
                      StockCode: String,
                      Description: String,
                      Quantity: BigInt,
                      InvoiceDate: String,
                      UnitPrice: Double,
                      CustomerID: Double,
                      Country: String)
