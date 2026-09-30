module "object_storage" {
  source = "../../modules/gcp_object_storage"

  project_id  = var.project_id
  location    = var.region
  bucket_name = var.bucket_name
}

module "network" {
  count = var.create_network ? 1 : 0

  source = "../../modules/gcp_vpc"

  project_id  = var.project_id
  name        = var.gke_network_name
  region      = var.region
  subnet_cidr = var.gke_subnet_cidr
}

locals {
  gke_network    = var.create_network ? module.network[0].network_name : var.gke_network
  gke_subnetwork = var.create_network ? module.network[0].subnetwork_name : var.gke_subnetwork
}

module "gke" {
  count = var.enable_gke ? 1 : 0

  source = "../../modules/gcp_gke"

  name         = var.gke_name
  project_id   = var.project_id
  location     = var.region
  network      = local.gke_network
  subnetwork   = local.gke_subnetwork
  node_count   = var.gke_node_count
  machine_type = var.gke_machine_type
}
