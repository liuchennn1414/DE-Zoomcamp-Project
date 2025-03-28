variable "GOOGLE_PROJECT_ID" {
  description = "GOOGLE_PROJECT_ID"
  type        = string
}

variable "region" {
  description = "Region"
  default = "europe-west1-b"
}

variable "location" {
  description = "Project Location"
  default = "EU"
}

variable "GOOGLE_BQ_DATASET" {
  description = "My BigQuery Dataset Name"
  type        = string
}

variable "GOOGLE_BUCKET_NAME" {
  description = "My Storage Bucket Name"
  type        = string
}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"
}