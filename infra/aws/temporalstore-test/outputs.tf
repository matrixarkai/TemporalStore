output "account_id" {
  value = data.aws_caller_identity.current.account_id
}

output "region" {
  value = var.aws_region
}

output "vpc_id" {
  value = aws_vpc.this.id
}

output "subnet_id" {
  value = aws_subnet.public.id
}

output "efs_id" {
  value = aws_efs_file_system.shared.id
}

output "data_nodes" {
  value = {
    for name, instance in aws_instance.data :
    name => {
      id         = instance.id
      private_ip = instance.private_ip
      public_ip  = instance.public_ip
      cache_ebs  = aws_ebs_volume.data_cache[name].id
    }
  }
}

output "metaservers" {
  value = {
    for name, instance in aws_instance.metaserver :
    name => {
      id             = instance.id
      private_ip     = instance.private_ip
      public_ip      = instance.public_ip
      monitoring_url = "http://${instance.public_ip}:8088/"
    }
  }
}

output "monitoring_urls" {
  value = {
    for name, instance in aws_instance.metaserver :
    name => "http://${instance.public_ip}:8088/"
  }
}

output "ssm_commands" {
  value = {
    data_1 = "aws ssm start-session --profile ${var.aws_profile} --region ${var.aws_region} --target ${aws_instance.data["data-01"].id}"
    meta_1 = "aws ssm start-session --profile ${var.aws_profile} --region ${var.aws_region} --target ${aws_instance.metaserver["meta-01"].id}"
  }
}
