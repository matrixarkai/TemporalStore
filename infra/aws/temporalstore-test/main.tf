locals {
  common_tags = {
    Project     = "TemporalStore"
    Environment = "test"
    ManagedBy   = "terraform"
  }

  data_nodes = {
    for idx in range(var.data_node_count) :
    format("data-%02d", idx + 1) => idx
  }

  metaservers = {
    for idx in range(var.metaserver_count) :
    format("meta-%02d", idx + 1) => idx
  }
}

data "aws_caller_identity" "current" {}

data "aws_ami" "ubuntu2204" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_vpc" "this" {
  cidr_block           = "10.70.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.name_prefix}-vpc"
  }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = "${var.name_prefix}-igw"
  }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.this.id
  cidr_block              = "10.70.1.0/24"
  availability_zone       = var.availability_zone
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.name_prefix}-public-${var.availability_zone}"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = {
    Name = "${var.name_prefix}-public-rt"
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "nodes" {
  name        = "${var.name_prefix}-nodes"
  description = "TemporalStore node-to-node and outbound access"
  vpc_id      = aws_vpc.this.id

  tags = {
    Name = "${var.name_prefix}-nodes"
  }
}

resource "aws_security_group_rule" "nodes_self_all" {
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.nodes.id
  self              = true
  description       = "Allow all TemporalStore intra-cluster traffic"
}

resource "aws_security_group_rule" "ssh_admin_optional" {
  count             = var.allowed_admin_cidr == "" ? 0 : 1
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = [var.allowed_admin_cidr]
  security_group_id = aws_security_group.nodes.id
  description       = "Optional temporary SSH access"
}

resource "aws_security_group_rule" "monitoring_ui_optional" {
  count             = var.allowed_monitoring_cidr == "" ? 0 : 1
  type              = "ingress"
  from_port         = 8088
  to_port           = 8088
  protocol          = "tcp"
  cidr_blocks       = [var.allowed_monitoring_cidr]
  security_group_id = aws_security_group.nodes.id
  description       = "Optional TemporalStore monitoring UI access on metaserver"
}

resource "aws_security_group_rule" "nodes_egress_all" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  ipv6_cidr_blocks  = ["::/0"]
  security_group_id = aws_security_group.nodes.id
  description       = "Allow outbound internet access for packages and SSM"
}

resource "aws_security_group" "efs" {
  name        = "${var.name_prefix}-efs"
  description = "TemporalStore EFS mount access"
  vpc_id      = aws_vpc.this.id

  tags = {
    Name = "${var.name_prefix}-efs"
  }
}

resource "aws_security_group_rule" "efs_from_nodes" {
  type                     = "ingress"
  from_port                = 2049
  to_port                  = 2049
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.nodes.id
  security_group_id        = aws_security_group.efs.id
  description              = "Allow NFS from TemporalStore nodes"
}

resource "aws_security_group_rule" "efs_egress_all" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.efs.id
}

resource "aws_iam_role" "ec2" {
  name = "${var.name_prefix}-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "artifact_read" {
  name = "${var.name_prefix}-artifact-read"
  role = aws_iam_role.ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject"
      ]
      Resource = "arn:aws:s3:::temporalstore-test-artifacts-${data.aws_caller_identity.current.account_id}-${var.aws_region}/temporalstore-runtime-release-mtcache-ssd.tar.gz"
    }]
  })
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${var.name_prefix}-ec2-profile"
  role = aws_iam_role.ec2.name
}

resource "aws_efs_file_system" "shared" {
  creation_token   = "${var.name_prefix}-shared"
  performance_mode = "generalPurpose"
  throughput_mode  = "elastic"
  encrypted        = true

  lifecycle_policy {
    transition_to_ia = "AFTER_30_DAYS"
  }

  tags = {
    Name = "${var.name_prefix}-shared"
  }
}

resource "aws_efs_mount_target" "shared" {
  file_system_id  = aws_efs_file_system.shared.id
  subnet_id       = aws_subnet.public.id
  security_groups = [aws_security_group.efs.id]
}

resource "aws_instance" "data" {
  for_each = local.data_nodes

  ami                         = data.aws_ami.ubuntu2204.id
  instance_type               = var.data_node_instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.nodes.id]
  iam_instance_profile        = aws_iam_instance_profile.ec2.name
  associate_public_ip_address = true

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_gb
    delete_on_termination = true
    encrypted             = true
  }

  user_data = templatefile("${path.module}/user_data_data_node.sh.tftpl", {
    efs_id = aws_efs_file_system.shared.id
    region = var.aws_region
  })

  depends_on = [aws_efs_mount_target.shared]

  tags = {
    Name = "${var.name_prefix}-${each.key}"
    Role = "data"
  }
}

resource "aws_ebs_volume" "data_cache" {
  for_each = local.data_nodes

  availability_zone = var.availability_zone
  size              = var.data_cache_volume_gb
  type              = "gp3"
  encrypted         = true

  tags = {
    Name = "${var.name_prefix}-${each.key}-ssd-cache"
    Role = "ssd-cache"
  }
}

resource "aws_volume_attachment" "data_cache" {
  for_each = local.data_nodes

  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.data_cache[each.key].id
  instance_id = aws_instance.data[each.key].id
}

resource "aws_instance" "metaserver" {
  for_each = local.metaservers

  ami                         = data.aws_ami.ubuntu2204.id
  instance_type               = var.metaserver_instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.nodes.id]
  iam_instance_profile        = aws_iam_instance_profile.ec2.name
  associate_public_ip_address = true

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_gb
    delete_on_termination = true
    encrypted             = true
  }

  user_data = templatefile("${path.module}/user_data_common.sh.tftpl", {
    efs_id = aws_efs_file_system.shared.id
    region = var.aws_region
  })

  depends_on = [aws_efs_mount_target.shared]

  tags = {
    Name = "${var.name_prefix}-${each.key}"
    Role = "metaserver"
  }
}
