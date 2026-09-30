# Terraform infrastructure

Terraform is the infrastructure control plane for the fraud platform. The local stack stays cloud-agnostic while AWS, Azure and GCP resources are defined under separate environment roots and reusable provider-specific modules.

Layout:

infrastructure/terraform/
  modules/aws_object_storage
  modules/azure_object_storage
  modules/gcp_object_storage
  environments/local
  environments/aws
  environments/azure
  environments/gcp

Provider baselines:
- Terraform 1.16.4
- AWS 6.61.0
- AzureRM 5.7.0
- Google 8.2.0

CI validates configuration only. It never applies cloud infrastructure. Cloud credentials must come from the operator's authenticated environment. HashiCorp recommends explicit provider version constraints and committing .terraform.lock.hcl for reproducible provider selection.