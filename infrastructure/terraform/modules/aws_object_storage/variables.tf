variable "bucket_name" {
  type        = string
  description = "Globally unique S3 bucket name."
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "Tags applied to the bucket."
}
