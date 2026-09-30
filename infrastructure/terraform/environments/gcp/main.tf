module "object_storage" {
  source = "../../modules/gcp_object_storage"

  project_id  = var.project_id
  location    = var.region
  bucket_name = var.bucket_name
}
