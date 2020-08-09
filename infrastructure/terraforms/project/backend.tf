terraform {
  backend "gcs" {
    prefix = "project/state"
  }
}