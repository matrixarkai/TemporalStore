variable "aws_profile" {
  description = "Local AWS CLI profile to use for deployment."
  type        = string
  default     = "temporalstore"
}

variable "aws_region" {
  description = "AWS region for TemporalStore resources."
  type        = string
  default     = "us-west-2"
}

variable "name_prefix" {
  description = "Prefix for named AWS resources."
  type        = string
  default     = "temporalstore-test"
}

variable "availability_zone" {
  description = "Single AZ for the first small functional test."
  type        = string
  default     = "us-west-2a"
}

variable "data_node_count" {
  type    = number
  default = 2
}

variable "metaserver_count" {
  type    = number
  default = 1
}

variable "data_node_instance_type" {
  type    = string
  default = "c7i.large"
}

variable "metaserver_instance_type" {
  type    = string
  default = "t3.small"
}

variable "root_volume_gb" {
  type    = number
  default = 20
}

variable "data_cache_volume_gb" {
  description = "Extra gp3 EBS volume per data node for the MtCache SSD tier."
  type        = number
  default     = 10
}

variable "allowed_admin_cidr" {
  description = "Optional CIDR for temporary SSH access. Leave empty to keep SSH closed."
  type        = string
  default     = ""
}

variable "allowed_monitoring_cidr" {
  description = "Optional CIDR for TemporalStore monitoring UI access on port 8088. Leave empty to keep it private to the cluster."
  type        = string
  default     = ""
}
