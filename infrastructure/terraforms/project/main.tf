provider "google" {
  project     = "sankir-1705"
  region      = "us-central1"
  zone        = "us-central1-c"
}

resource "google_bigquery_dataset" "audit_dataset" {
  dataset_id                  = "audit"
  friendly_name               = "audit"
  description                 = "This is a Audit Dataset"
  location                    = "US"
}

resource "google_bigquery_dataset" "dl" {
  dataset_id                  = "dl"
  friendly_name               = "dl"
  description                 = "This is a Data Lake Dataset"
  location                    = "US"
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id                  = "analytics"
  friendly_name               = "analytics"
  description                 = "This is a analytics Dataset"
  location                    = "US"
}


resource "google_compute_instance" "default" {
  name         = "test"
  machine_type = "n1-standard-1"
  zone         = "us-central1-a"

  tags = ["foo", "bar"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-9"
    }
  }

  // Local SSD disk
  scratch_disk {
    interface = "SCSI"
  }

  network_interface {
    network = "default"

  }

  metadata = {
    foo = "bar"
  }
  metadata_startup_script = "echo hi > /test.txt"
}