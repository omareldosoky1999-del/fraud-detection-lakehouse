output "lakehouse_bucket" {
  value = module.object_storage.bucket_name
}

output "lakehouse_bucket_url" {
  value = module.object_storage.bucket_url
}

output "gke_cluster_name" {
  value = var.enable_gke ? module.gke[0].cluster_name : null
}

output "gke_workload_pool" {
  value = var.enable_gke ? module.gke[0].workload_pool : null
}
