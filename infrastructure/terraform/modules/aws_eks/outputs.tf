output "cluster_name" { value = aws_eks_cluster.this.name }
output "cluster_arn" { value = aws_eks_cluster.this.arn }
output "cluster_endpoint" { value = aws_eks_cluster.this.endpoint }
output "oidc_issuer" { value = aws_eks_cluster.this.identity[0].oidc[0].issuer }
output "node_group_name" { value = aws_eks_node_group.this.node_group_name }
