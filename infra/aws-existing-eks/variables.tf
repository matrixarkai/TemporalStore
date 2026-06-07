variable "aws_region" {
  description = "AWS region containing the existing EKS cluster."
  type        = string
}

variable "aws_profile" {
  description = "Optional AWS CLI profile to use."
  type        = string
  default     = ""
}

variable "eks_cluster_name" {
  description = "Name of an existing EKS cluster. Terraform reuses this cluster and does not create a new one."
  type        = string
}

variable "namespace" {
  description = "Kubernetes namespace for TemporalStore."
  type        = string
  default     = "temporalstore"
}

variable "image" {
  description = "Container image containing metaserver, server, proxy, and redis_proxy release binaries."
  type        = string
}

variable "image_pull_policy" {
  description = "Kubernetes image pull policy."
  type        = string
  default     = "IfNotPresent"
}

variable "shard_id" {
  description = "Single-node shard id."
  type        = number
  default     = 1
}

variable "storage_class_name" {
  description = "Existing Kubernetes StorageClass for the server PVC. Empty uses the cluster default."
  type        = string
  default     = ""
}

variable "server_storage_size" {
  description = "PVC size for local cache, page store, and index data."
  type        = string
  default     = "20Gi"
}

variable "cache_memory_bytes" {
  description = "L1 memory cache size for temporalstore server."
  type        = number
  default     = 16777216
}

variable "proxy_service_type" {
  description = "Service type for the HTTP proxy."
  type        = string
  default     = "LoadBalancer"
}

variable "redis_service_type" {
  description = "Service type for the Redis-compatible proxy."
  type        = string
  default     = "LoadBalancer"
}

variable "labels" {
  description = "Extra labels applied to Kubernetes resources."
  type        = map(string)
  default     = {}
}
