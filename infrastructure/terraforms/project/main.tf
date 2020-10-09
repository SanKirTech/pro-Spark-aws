provider "google" {
  project     = "pro-spark"
  region      = "us-central1"
  zone        = "us-central1-c"
}



//resource "google_compute_instance" "default" {
//  name         = "test"
//  machine_type = "n1-standard-1"
//  zone         = "us-central1-a"
//
//  tags = ["foo", "bar"]
//
//  boot_disk {
//    initialize_params {
//      image = "debian-cloud/debian-9"
//    }
//  }
//
//  // Local SSD disk
//  scratch_disk {
//    interface = "SCSI"
//  }
//
//  network_interface {
//    network = "default"
//
//  }
//
//  metadata = {
//    foo = "bar"
//  }
//  metadata_startup_script = "echo hi > /test.txt"
//}