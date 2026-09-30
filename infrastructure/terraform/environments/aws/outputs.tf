output "lakehouse_bucket" {
  value = module.object_storage.bucket_name
}

output "lakehouse_bucket_arn" {
  value = module.object_storage.bucket_arn
}
