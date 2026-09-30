resource "aws_vpc" "this" {
  cidr_block           = var.cidr_block
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = var.name
  }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = "${var.name}-igw"
  }
}

resource "aws_subnet" "public" {
  for_each = {
    for idx, cidr in var.public_subnet_cidrs :
    tostring(idx) => {
      cidr = cidr
      az   = var.availability_zones[idx]
    }
  }

  vpc_id                  = aws_vpc.this.id
  cidr_block              = each.value.cidr
  availability_zone       = each.value.az
  map_public_ip_on_launch = true

  tags = merge(
    {
      Name                     = "${var.name}-public-${each.key}"
      "kubernetes.io/role/elb" = "1"
    },
    var.cluster_name == "" ? {} : {
      "kubernetes.io/cluster/${var.cluster_name}" = "shared"
    },
  )
}

resource "aws_subnet" "private" {
  for_each = {
    for idx, cidr in var.private_subnet_cidrs :
    tostring(idx) => {
      cidr = cidr
      az   = var.availability_zones[idx]
    }
  }

  vpc_id            = aws_vpc.this.id
  cidr_block        = each.value.cidr
  availability_zone = each.value.az

  tags = merge(
    {
      Name                              = "${var.name}-private-${each.key}"
      "kubernetes.io/role/internal-elb" = "1"
    },
    var.cluster_name == "" ? {} : {
      "kubernetes.io/cluster/${var.cluster_name}" = "shared"
    },
  )
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = {
    Name = "${var.name}-public-rt"
  }
}

resource "aws_route_table_association" "public" {
  for_each = aws_subnet.public

  subnet_id      = each.value.id
  route_table_id = aws_route_table.public.id
}

resource "aws_eip" "nat" {
  count  = var.single_nat_gateway ? 1 : length(var.public_subnet_cidrs)
  domain = "vpc"
}

resource "aws_nat_gateway" "single" {
  count = var.single_nat_gateway ? 1 : 0

  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.public["0"].id

  depends_on = [aws_internet_gateway.this]

  tags = {
    Name = "${var.name}-nat-1"
  }
}

resource "aws_nat_gateway" "per_az" {
  for_each = var.single_nat_gateway ? {} : aws_subnet.public

  allocation_id = aws_eip.nat[tonumber(each.key)].id
  subnet_id     = each.value.id

  depends_on = [aws_internet_gateway.this]

  tags = {
    Name = "${var.name}-nat-${each.key}"
  }
}

resource "aws_route_table" "private" {
  for_each = aws_subnet.private

  vpc_id = aws_vpc.this.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = var.single_nat_gateway ? aws_nat_gateway.single[0].id : aws_nat_gateway.per_az[each.key].id
  }

  tags = {
    Name = "${var.name}-private-rt-${each.key}"
  }
}

resource "aws_route_table_association" "private" {
  for_each = aws_subnet.private

  subnet_id      = each.value.id
  route_table_id = aws_route_table.private[each.key].id
}