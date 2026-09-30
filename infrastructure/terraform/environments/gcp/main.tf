module "object_storage" {
  source = "../../modules/gcp_object_storage"

  project_id  = var.project_id
  location    = var.region
  bucket_name = var.bucket_name
}

module "gke" {
  count = var.enable_gke ? 1 : 0

  source = "../../modules/gcp_gke"

  name          = var.gke_name
  project_id    = var.project_id
  location      = var.region
  network       = var.gke_network
  subnetwork    = var.gke_subnetwork
  node_count    = var.gke_node_count
  machine_type  = var.gke_machine_type
}
