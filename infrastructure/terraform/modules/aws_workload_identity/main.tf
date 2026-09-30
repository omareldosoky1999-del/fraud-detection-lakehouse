data "aws_iam_policy_document" "assume_role" {
  statement {
    sid    = "AllowEksPodIdentity"
    effect = "Allow"
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  name = var.name
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
  description = "Least-privilege EKS Pod Identity role for fraud lakehouse Spark workloads."
}

data "aws_iam_policy_document" "s3" {
  statement {
    sid = "ListLakehouseBucket"
    effect = "Allow"
    actions = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"]
    resources = [var.bucket_arn]
  }
  statement {
    sid = "ReadWriteLakehouseObjects"
    effect = "Allow"
    actions = ["s3:AbortMultipartUpload", "s3:DeleteObject", "s3:GetObject", "s3:ListMultipartUploadParts", "s3:PutObject"]
    resources = ["${var.bucket_arn}/*"]
  }
}

resource "aws_iam_role_policy" "s3" {
  name = "${var.name}-s3"
  role = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.s3.json
}

resource "aws_eks_pod_identity_association" "this" {
  cluster_name = var.cluster_name
  namespace = var.namespace
  service_account = var.service_account
  role_arn = aws_iam_role.this.arn
}
