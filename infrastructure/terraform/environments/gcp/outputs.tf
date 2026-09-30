output "lakehouse_bucket" {
  value = module.object_storage.bucket_name
}

output "lakehouse_bucket_url" {
  value = module.object_storage.bucket_url
}
