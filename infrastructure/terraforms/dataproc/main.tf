provider "google" {
  credentials = file("sankir-1705-SA.json")
  project     = "sankir-1705"
  region      = "us-central1"
  zone        = "us-central1-c"
}

resource "google_dataproc_cluster" "mycluster" {
  name     = "sankir-sanjay-kiran"
  region   = "us-central1"
  labels = {
    foo = "bar"
  }

  cluster_config {
    # staging_bucket = "dataproc-staging-bucket"

    master_config {
      num_instances = 1
      machine_type  = "n1-standard-4"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 15
      }
    }

    worker_config {
      num_instances    = 0
    }

    preemptible_worker_config {
      num_instances = 0
    }

    # Override or set some custom properties
    software_config {
      image_version = "1.3"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

    gce_cluster_config {
      tags = ["foo", "bar"]
      service_account ="sankir-terraform-01@sankir-1705.iam.gserviceaccount.com"
      service_account_scopes = [
        "https://www.googleapis.com/auth/monitoring",
        "useraccounts-ro",
        "storage-rw",
        "logging-write",
      ]
    }

  }
}
