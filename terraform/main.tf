terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.16.0"
    }
  }
}

provider "google" {
  # Configuration options
  project = var.GOOGLE_PROJECT_ID
  region  = var.region
}

resource "google_storage_bucket" "test-bucket" {
  name          = var.GOOGLE_BUCKET_NAME
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "test_dataset" {
  dataset_id = var.GOOGLE_BQ_DATASET
  location = var.location

}