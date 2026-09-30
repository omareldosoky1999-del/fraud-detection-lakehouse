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

## Workload identity contract

The cloud environments do not place long-lived cloud credentials in the Spark container.

| Cloud | Kubernetes identity | Object-store permission | Terraform output used by Helm |
| --- | --- | --- | --- |
| AWS | EKS Pod Identity | S3 object read/write on the lakehouse bucket | `workload_identity_role_arn` |
| Azure | AKS Workload Identity | Storage Blob Data Contributor on the storage account | `workload_identity_client_id` |
| GCP | GKE Workload Identity Federation | Storage Object Admin on the lakehouse bucket | `workload_identity_service_account_email` |

AWS uses the EKS Pod Identity Agent and an `aws_eks_pod_identity_association`; no ServiceAccount annotation is required. Azure and GCP require the corresponding Kubernetes ServiceAccount identity reference at deployment time. The Helm deployment workflow accepts this value as `workload_identity_ref`.

The IAM bindings are intentionally scoped to the single lakehouse object store used by the platform rather than broad project/account-wide administrator roles. Kubernetes namespace and service-account names default to `fraud-platform` and `fraud-spark` and can be overridden through Terraform variables.
