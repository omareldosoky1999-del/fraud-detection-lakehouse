# Terraform infrastructure

Terraform is the infrastructure control plane for the fraud platform. The local stack stays cloud-agnostic while AWS, Azure and GCP resources are defined under separate environment roots and reusable provider-specific modules.

Layout:

infrastructure/terraform/
  modules/
    aws_object_storage
    aws_vpc
    aws_eks
    azure_object_storage
    azure_vnet
    azure_aks
    gcp_object_storage
    gcp_vpc
    gcp_gke
  environments/
    local
    aws
    azure
    gcp

Provider baselines:

- Terraform 1.16.4
- AWS 6.61.0
- AzureRM 5.7.0
- Google 8.2.0

The cloud environments are opt-in. Object storage can be provisioned independently, while the managed Kubernetes clusters can either consume existing subnet/network IDs or use the included networking modules.

CI validates configuration only. It never applies cloud infrastructure. Cloud credentials must come from the operator's authenticated environment.

The AWS module creates public/private subnets and NAT routing. Azure creates a VNet and AKS subnet. GCP creates a custom VPC and regional subnet. The network modules are disabled by default to avoid accidental cloud provisioning.
