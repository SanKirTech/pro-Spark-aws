resource "google_bigquery_dataset" "retail" {
  dataset_id                  = "retail_bq"
  friendly_name               = "retail_bq"
  description                 = "This is a Retail Dataset"
  location                    = "US"
}

resource "google_bigquery_dataset" "retail_analytics" {
  dataset_id                  = "retail_analytics"
  friendly_name               = "retail_analytics"
  description                 = "This is retail_analytics Dataset"
  location                    = "US"
}

resource "google_bigquery_table" "retail_transaction" {
  dataset_id = google_bigquery_dataset.retail.dataset_id
  table_id   = "t_transaction"

//  time_partitioning {
//    type = "DAY"
//    field = "InvoiceDate"
//  }

  labels = {
    env = "default"
  }

  schema = file("bigquery-schema/t_transaction.json")

}

resource "google_bigquery_table" "error" {
  dataset_id = google_bigquery_dataset.retail.dataset_id
  table_id   = "t_error"

  labels = {
    env = "default"
  }

  schema = file("bigquery-schema/t_error.json")
}
