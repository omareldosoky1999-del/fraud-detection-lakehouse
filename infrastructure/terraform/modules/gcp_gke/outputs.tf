output "cluster_name" { value = google_container_cluster.this.name }
output "cluster_endpoint" { value = google_container_cluster.this.endpoint }
output "workload_pool" { value = "${var.project_id}.svc.id.goog" }
output "node_pool_name" { value = google_container_node_pool.this.name }
