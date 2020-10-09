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

resource "google_bigquery_dataset" "retail_kpi" {
  dataset_id                  = "retail_kpi"
  friendly_name               = "retail_kpi"
  description                 = "This is retail_kpi Dataset"
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

//++ KPI Tables Below //
resource "google_bigquery_table" "sku_dow_daily1" {
  dataset_id = google_bigquery_dataset.retail_kpi.dataset_id
  table_id   = "t_sku_dow_daily1"

  labels = {
    env = "default"
  }

  schema = file("bigquery-schema/t_sku_dow_daily1.json")
}

resource "google_bigquery_table" "t_sku_revenue_w_dly" {
  dataset_id = google_bigquery_dataset.retail_kpi.dataset_id
  table_id   = "t_sku_revenue_w_dly"

  labels = {
    env = "default"
  }

  schema = file("bigquery-schema/t_sku_revenue_w_dly.json")
}

resource "google_bigquery_table" "t_sku_revenue_c_dly" {
  dataset_id = google_bigquery_dataset.retail_kpi.dataset_id
  table_id   = "t_sku_revenue_c_dly"

  labels = {
    env = "default"
  }

  schema = file("bigquery-schema/t_sku_revenue_c_dly.json")
}

resource "google_bigquery_table" "t_sales_anomoly_dly" {
  dataset_id = google_bigquery_dataset.retail_kpi.dataset_id
  table_id   = "t_sales_anomoly_dly"

  labels = {
    env = "default"
  }

  schema = file("bigquery-schema/t_sales_anomoly_dly.json")
}

resource "google_bigquery_table" "t_customer_rank_dly" {
  dataset_id = google_bigquery_dataset.retail_kpi.dataset_id
  table_id   = "t_customer_rank_dly"

  labels = {
    env = "default"
  }

  schema = file("bigquery-schema/t_customer_rank_dly.json")
}

resource "google_bigquery_table" "t_revenue_country_dly" {
  dataset_id = google_bigquery_dataset.retail_kpi.dataset_id
  table_id   = "t_revenue_country_dly"

  labels = {
    env = "default"
  }

  schema = file("bigquery-schema/t_revenue_country_dly.json")
}

resource "google_bigquery_table" "t_revenue_qtr_dly" {
  dataset_id = google_bigquery_dataset.retail_kpi.dataset_id
  table_id   = "t_revenue_qtr_dly"

  labels = {
    env = "default"
  }

  schema = file("bigquery-schema/t_revenue_qtr_dly.json")
}

resource "google_bigquery_table" "t_revenue_country_qtr" {
  dataset_id = google_bigquery_dataset.retail_kpi.dataset_id
  table_id   = "t_revenue_country_qtr_dly"

  labels = {
    env = "default"
  }

  schema = file("bigquery-schema/t_revenue_country_qtr_dly.json")
}

/* Summary Table creation begins */
resource "google_bigquery_table" "sku_dow_summary" {
  dataset_id = google_bigquery_dataset.retail_kpi.dataset_id
  table_id   = "t_sku_dow_summary"

  labels = {
    env = "default"
  }

  schema = file("bigquery-schema/t_sku_dow_summary.json")
}

resource "google_bigquery_table" "t_sku_revenue_w_summary" {
  dataset_id = google_bigquery_dataset.retail_kpi.dataset_id
  table_id   = "t_sku_revenue_w_summary"

  labels = {
    env = "default"
  }

  schema = file("bigquery-schema/t_sku_revenue_w_summary.json")
}

resource "google_bigquery_table" "t_sku_revenue_c_summary" {
  dataset_id = google_bigquery_dataset.retail_kpi.dataset_id
  table_id   = "t_sku_revenue_c_summary"

  labels = {
    env = "default"
  }

  schema = file("bigquery-schema/t_sku_revenue_c_summary.json")
}
/* Summary Table creation Ends */
resource "google_bigquery_table" "sku_dow_kiran" {
  dataset_id = google_bigquery_dataset.retail_kpi.dataset_id
  table_id = "t_sku_dow_kiran"

  labels = {
    env = "default"
  }

  schema = file("bigquery-schema/kiran.json")
}