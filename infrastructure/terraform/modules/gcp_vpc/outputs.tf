output "network_id" {
  value = google_compute_network.this.id
}

output "network_name" {
  value = google_compute_network.this.name
}

output "subnetwork_id" {
  value = google_compute_subnetwork.this.id
}

output "subnetwork_name" {
  value = google_compute_subnetwork.this.name
}
